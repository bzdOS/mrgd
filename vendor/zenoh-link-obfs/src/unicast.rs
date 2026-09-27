// START_AI_HEADER
// MODULE: mac-companion/zenoh-link-obfs/src/unicast.rs
// PURPOSE: Zenoh unicast LinkManager / Link implementation for the "obfs" transport, mirroring zenoh-link-tls structure.
// INTENT: Wire the AEAD-framed ObfsStream into Zenoh's unicast link infrastructure so it can be used as a drop-in replacement for TLS/plain TCP transports.
// DEPENDENCIES: async_trait, tokio (TcpListener, TcpStream, AsyncMutex), tokio_util (CancellationToken), zenoh_core, zenoh_link_commons, zenoh_protocol, zenoh_result, crate::crypto (ObfsStream, StaticServerKey), crate (load_psk, config constants)
// PUBLIC_API: LinkUnicastObfs, LinkManagerUnicastObfs
// END_AI_HEADER

//
// bsdOS — zenoh-link-obfs
//
// Zenoh unicast LinkManager / Link implementation for the "obfs" transport.
// Structurally modelled on zenoh-link-tls-patched/src/unicast.rs, but the TLS
// session is replaced by the AEAD-framed ObfsStream from crypto.rs.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
use std::{
    cell::UnsafeCell,
    convert::TryInto,
    fmt,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Mutex as AsyncMutex,
};
use tokio_util::sync::CancellationToken;
use zenoh_core::zasynclock;
use zenoh_link_commons::{
    get_ip_interface_names, parse_dscp, tcp::TcpSocketConfig, LinkAuthId,
    LinkManagerUnicastTrait, LinkUnicast, LinkUnicastTrait, ListenersUnicastIP,
    NewLinkChannelSender, BIND_INTERFACE, BIND_SOCKET, TCP_SO_RCV_BUF, TCP_SO_SND_BUF,
};
use zenoh_protocol::{
    core::{
        endpoint::{Address, Config},
        EndPoint, Locator, Priority,
    },
    transport::BatchSize,
};
use zenoh_result::{bail, zerror, ZResult};

use crate::{
    crypto::{ObfsStream, StaticServerKey},
    load_psk, OBFS_ACCEPT_THROTTLE_TIME, OBFS_DEFAULT_MTU, OBFS_LINGER_TIMEOUT,
    OBFS_LOCATOR_PREFIX,
};

/// A single obfuscated unicast link.
///
/// NOTE on `UnsafeCell`: `ObfsStream` requires `&mut self` for read and write.
/// Concurrent reads and writes are prevented by `read_mtx` / `write_mtx`, exactly
/// as in `zenoh-link-tls-patched`. There is at most one reader task and one writer
/// task per link, and they touch disjoint cipher state (recv vs send), so interior
/// mutability through `UnsafeCell` is sound here. This mirrors the upstream TLS link.
pub struct LinkUnicastObfs {
    inner: UnsafeCell<ObfsStream<TcpStream>>,
    src_addr: SocketAddr,
    src_locator: Locator,
    dst_addr: SocketAddr,
    dst_locator: Locator,
    write_mtx: AsyncMutex<()>,
    read_mtx: AsyncMutex<()>,
    auth_identifier: LinkAuthId,
    mtu: BatchSize,
}

// SAFETY: access to the UnsafeCell is serialized through read_mtx/write_mtx, and
// reader/writer touch disjoint state. Same justification as the TLS link.
unsafe impl Send for LinkUnicastObfs {}
unsafe impl Sync for LinkUnicastObfs {}

impl LinkUnicastObfs {
    /// Build a link from an already-handshaked `ObfsStream`.
    ///
    /// Applies best-effort TCP socket tuning (NODELAY, LINGER) on the owned inner
    /// stream and computes the MTU per IETF RFC6691, reserving the AEAD per-frame
    /// overhead so a full Zenoh batch always fits one record.
    // from_stream:start
//   purpose: Build a LinkUnicastObfs from an already-handshaked ObfsStream, applying TCP socket tuning and computing the MTU.
//   input:  stream: ObfsStream<TcpStream> — the handshaked AEAD stream; src_addr: SocketAddr — local socket address; dst_addr: SocketAddr — remote socket address
//   output: ZResult<LinkUnicastObfs> — the Zenoh link, or error on locator construction failure
//   sideEffects: Sets TCP_NODELAY and SO_LINGER on the inner TcpStream
    fn from_stream(
        mut stream: ObfsStream<TcpStream>,
        src_addr: SocketAddr,
        dst_addr: SocketAddr,
    ) -> ZResult<LinkUnicastObfs> {
        {
            let inner = stream.inner_mut();
            if let Err(err) = inner.set_nodelay(true) {
                tracing::warn!(
                    "Unable to set NODELAY on obfs link {} => {}: {}",
                    src_addr,
                    dst_addr,
                    err
                );
            }
            #[allow(deprecated)]
            if let Err(err) = inner.set_linger(Some(Duration::from_secs(
                (*OBFS_LINGER_TIMEOUT).try_into().unwrap_or(10u64),
            ))) {
                tracing::warn!(
                    "Unable to set LINGER on obfs link {} => {}: {}",
                    src_addr,
                    dst_addr,
                    err
                );
            }
        }

        let ip_header = match src_addr.ip() {
            std::net::IpAddr::V4(_) => 40,
            std::net::IpAddr::V6(_) => 60,
        };
        let mtu = (*OBFS_DEFAULT_MTU)
            .saturating_sub(ip_header)
            .saturating_sub(crate::OBFS_FRAME_OVERHEAD);

        // Locator::new returns ZResult; building from a SocketAddr string cannot
        // realistically fail, but we propagate rather than unwrap (project rule).
        let src_locator = Locator::new(OBFS_LOCATOR_PREFIX, src_addr.to_string(), "")?;
        let dst_locator = Locator::new(OBFS_LOCATOR_PREFIX, dst_addr.to_string(), "")?;

        Ok(LinkUnicastObfs {
            inner: UnsafeCell::new(stream),
            src_addr,
            src_locator,
            dst_addr,
            dst_locator,
            write_mtx: AsyncMutex::new(()),
            read_mtx: AsyncMutex::new(()),
            // obfs has no certificate identity; reuse the Tcp auth-id variant.
            auth_identifier: LinkAuthId::Tcp,
            mtu,
        })
    }
    // from_stream:end

    // NOTE: safe because read_mtx/write_mtx serialize every access. See struct doc.
    #[allow(clippy::mut_from_ref)]
    // get_mut_stream:start
//   purpose: Obtain a mutable reference to the inner ObfsStream through the UnsafeCell, relying on external mutex serialization.
//   input:  &self
//   output: &mut ObfsStream<TcpStream> — mutable reference to the framed stream
//   sideEffects: none
    fn get_mut_stream(&self) -> &mut ObfsStream<TcpStream> {
        unsafe { &mut *self.inner.get() }
    }
    // get_mut_stream:end

    // close_inner:start
//   purpose: Gracefully flush, then shutdown the underlying TCP stream.
//   input:  &self
//   output: ZResult<()> — Ok on clean shutdown, error on shutdown failure
//   sideEffects: Flushes ObfsStream and shuts down the inner TCP stream I/O
    async fn close_inner(&self) -> ZResult<()> {
        tracing::trace!("Closing obfs link: {}", self);
        let _guard = zasynclock!(self.write_mtx);
        let stream = self.get_mut_stream();
        let _ = stream.flush().await;
        let res = stream.inner_mut().shutdown().await;
        res.map_err(|e| zerror!(e).into())
    }
    // close_inner:end
}

#[async_trait]
impl LinkUnicastTrait for LinkUnicastObfs {
    // close:start
//   purpose: Close the obfs link gracefully, flushing pending writes and shutting down the TCP stream.
//   input:  &self
//   output: ZResult<()> — Ok on clean shutdown
//   sideEffects: Flushes and shuts down the underlying TCP stream
    async fn close(&self) -> ZResult<()> {
        self.close_inner().await
    }
    // close:end

    // write:start
//   purpose: Encrypt and write a buffer to the obfs link.
//   input:  buffer: &[u8] — the plaintext data; _priority: Option<Priority> — unused, retained for trait compat
//   output: ZResult<usize> — number of plaintext bytes written, or error
//   sideEffects: Encrypts data via ObfsStream and writes AEAD frames to TCP
    async fn write(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<usize> {
        let _guard = zasynclock!(self.write_mtx);
        self.get_mut_stream().write_frames(buffer).await.map_err(|e| {
            tracing::trace!("Write error on obfs link {}: {}", self, e);
            e
        })
    }
    // write:end

    // write_all:start
//   purpose: Encrypt and write the entire buffer to the obfs link.
//   input:  buffer: &[u8] — the plaintext data; _priority: Option<Priority> — unused, retained for trait compat
//   output: ZResult<()> — Ok on success, error if fewer bytes than buffer.len() were written
//   sideEffects: Encrypts data via ObfsStream and writes AEAD frames to TCP
    async fn write_all(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<()> {
        let _guard = zasynclock!(self.write_mtx);
        let stream = self.get_mut_stream();
        // write_frames already writes the entire buffer (looping internally).
        let n = stream.write_frames(buffer).await.map_err(|e| {
            tracing::trace!("Write error on obfs link {}: {}", self, e);
            e
        })?;
        if n != buffer.len() {
            bail!("obfs write_all wrote {n} of {} bytes", buffer.len());
        }
        Ok(())
    }
    // write_all:end

    // read:start
//   purpose: Read decrypted plaintext from the obfs link into the provided buffer.
//   input:  buffer: &mut [u8] — the buffer to fill; _priority: Option<Priority> — unused, retained for trait compat
//   output: ZResult<usize> — number of decrypted bytes read, 0 on clean EOF
//   sideEffects: Reads AEAD frames from TCP and decrypts them
    async fn read(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
        let _guard = zasynclock!(self.read_mtx);
        self.get_mut_stream().read_into(buffer).await.map_err(|e| {
            tracing::trace!("Read error on obfs link {}: {}", self, e);
            e
        })
    }
    // read:end

    // read_exact:start
//   purpose: Read exactly buffer.len() decrypted bytes from the obfs link.
//   input:  buffer: &mut [u8] — the buffer to fill exactly; _priority: Option<Priority> — unused, retained for trait compat
//   output: ZResult<()> — Ok on success, error on unexpected EOF
//   sideEffects: Reads AEAD frames from TCP and decrypts them
    async fn read_exact(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<()> {
        let _guard = zasynclock!(self.read_mtx);
        self.get_mut_stream().read_exact_into(buffer).await.map_err(|e| {
            tracing::trace!("Read error on obfs link {}: {}", self, e);
            e
        })
    }
    // read_exact:end

    #[inline(always)]
    // get_src:start
//   purpose: Return the source locator of the obfs link.
//   input:  &self
//   output: &Locator — the source locator
//   sideEffects: none
    fn get_src(&self) -> &Locator {
        &self.src_locator
    }
    // get_src:end

    #[inline(always)]
    // get_dst:start
//   purpose: Return the destination locator of the obfs link.
//   input:  &self
//   output: &Locator — the destination locator
//   sideEffects: none
    fn get_dst(&self) -> &Locator {
        &self.dst_locator
    }
    // get_dst:end

    #[inline(always)]
    // get_mtu:start
//   purpose: Return the MTU of the obfs link (Zenoh batch size).
//   input:  &self
//   output: BatchSize — the negotiated MTU
//   sideEffects: none
    fn get_mtu(&self) -> BatchSize {
        self.mtu
    }
    // get_mtu:end

    #[inline(always)]
    // get_interface_names:start
//   purpose: Return the network interface names associated with the source address.
//   input:  &self
//   output: Vec<String> — list of interface names
//   sideEffects: none
    fn get_interface_names(&self) -> Vec<String> {
        get_ip_interface_names(&self.src_addr)
    }
    // get_interface_names:end

    #[inline(always)]
    // is_reliable:start
//   purpose: Report whether the obfs link is a reliable transport.
//   input:  &self
//   output: bool — always true
//   sideEffects: none
    fn is_reliable(&self) -> bool {
        super::IS_RELIABLE
    }
    // is_reliable:end

    #[inline(always)]
    // is_streamed:start
//   purpose: Report whether the obfs link is a stream-oriented transport.
//   input:  &self
//   output: bool — always true
//   sideEffects: none
    fn is_streamed(&self) -> bool {
        true
    }
    // is_streamed:end

    #[inline(always)]
    // get_auth_id:start
//   purpose: Return the authentication identifier of the obfs link.
//   input:  &self
//   output: &LinkAuthId — the auth identifier (always LinkAuthId::Tcp for obfs)
//   sideEffects: none
    fn get_auth_id(&self) -> &LinkAuthId {
        &self.auth_identifier
    }
    // get_auth_id:end
}

impl Drop for LinkUnicastObfs {
    // drop:start
//   purpose: Shut down the underlying TCP stream when the link is dropped, blocking on the Acceptor runtime.
//   input:  &mut self
//   output: none
//   sideEffects: Shuts down the inner TCP stream
    fn drop(&mut self) {
        let stream = self.get_mut_stream();
        let _ = zenoh_runtime::ZRuntime::Acceptor
            .block_in_place(async move { stream.inner_mut().shutdown().await });
    }
    // drop:end
}

impl fmt::Display for LinkUnicastObfs {
    // fmt:start
//   purpose: Format the obfs link as "src_addr => dst_addr" for display.
//   input:  f: &mut fmt::Formatter<'_>
//   output: fmt::Result
//   sideEffects: none
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.src_addr, self.dst_addr)
    }
    // fmt:end
}

impl fmt::Debug for LinkUnicastObfs {
    // fmt:start
//   purpose: Format the obfs link as a debug struct with src and dst fields.
//   input:  f: &mut fmt::Formatter<'_>
//   output: fmt::Result
//   sideEffects: none
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Obfs")
            .field("src", &self.src_addr)
            .field("dst", &self.dst_addr)
            .finish()
    }
    // fmt:end
}

pub struct LinkManagerUnicastObfs {
    manager: NewLinkChannelSender,
    listeners: ListenersUnicastIP,
}

impl LinkManagerUnicastObfs {
    // new:start
//   purpose: Create a new obfs link manager with the given new-link channel sender.
//   input:  manager: NewLinkChannelSender — the channel to send newly accepted links to Zenoh
//   output: Self — the link manager
//   sideEffects: none
    pub fn new(manager: NewLinkChannelSender) -> Self {
        Self {
            manager,
            listeners: ListenersUnicastIP::new(),
        }
    }
    // new:end
}

/// Resolve an `obfs/<host:port>` address to a `SocketAddr`.
// get_obfs_addr:start
//   purpose: Resolve an obfs <host:port> address string to a SocketAddr via DNS lookup.
//   input:  address: &Address<'_> — the obfs endpoint address to resolve
//   output: ZResult<SocketAddr> — the resolved socket address, or error on DNS failure
//   sideEffects: Performs DNS lookup via tokio::net::lookup_host
async fn get_obfs_addr(address: &Address<'_>) -> ZResult<SocketAddr> {
    match tokio::net::lookup_host(address.as_str()).await?.next() {
        Some(addr) => Ok(addr),
        None => bail!("Couldn't resolve obfs locator address: {}", address),
    }
}
// get_obfs_addr:end

/// Extract the host portion of an `obfs/<host:port>` address (for the listener locator).
fn get_obfs_host<'a>(address: &'a Address<'a>) -> ZResult<&'a str> {
    Ok(address
        .as_str()
        .rsplit_once(':')
        .ok_or_else(|| zerror!("Invalid obfs address (expected host:port): {}", address.as_str()))?
        .0)
}

/// Build a `TcpSocketConfig` from the endpoint config parameters (so_sndbuf,
/// so_rcvbuf, iface/bind, dscp). Mirrors the TLS configurator's socket handling.
async fn tcp_socket_config_from<'a>(config: &'a Config<'_>) -> ZResult<TcpSocketConfig<'a>> {
    if let (Some(_), Some(_)) = (config.get(BIND_INTERFACE), config.get(BIND_SOCKET)) {
        bail!(
            "Using config options `{}` and `{}` together is unsupported",
            BIND_INTERFACE,
            BIND_SOCKET
        );
    }

    let mut tx_buffer_size = None;
    if let Some(size) = config.get(TCP_SO_SND_BUF) {
        tx_buffer_size = Some(
            size.parse()
                .map_err(|_| zerror!("Unknown TCP write buffer size argument: {}", size))?,
        );
    }
    let mut rx_buffer_size = None;
    if let Some(size) = config.get(TCP_SO_RCV_BUF) {
        rx_buffer_size = Some(
            size.parse()
                .map_err(|_| zerror!("Unknown TCP read buffer size argument: {}", size))?,
        );
    }
    let mut bind_socket = None;
    if let Some(bind_socket_str) = config.get(BIND_SOCKET) {
        bind_socket = Some(get_obfs_addr(&Address::from(bind_socket_str)).await?);
    }

    Ok(TcpSocketConfig::new(
        tx_buffer_size,
        rx_buffer_size,
        config.get(BIND_INTERFACE),
        bind_socket,
        parse_dscp(config)?,
    ))
}

#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastObfs {
    // new_link:start
//   purpose: Open a new obfs link to the given endpoint: resolve, connect, perform AEAD handshake.
//   input:  endpoint: EndPoint — the target endpoint with address and config (PSK)
//   output: ZResult<LinkUnicast> — the established obfs link, or error on connect/handshake failure
//   sideEffects: DNS lookup, TCP connect, AEAD handshake I/O
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        let epaddr = endpoint.address();
        let epconf = endpoint.config();

        let addr = get_obfs_addr(&epaddr).await?;
        let psk = load_psk(&epconf)?;
        let socket_config = tcp_socket_config_from(&epconf).await?;

        // Open the raw TCP connection.
        let (tcp_stream, src_addr, dst_addr) = socket_config.new_link(&addr).await.map_err(|e| {
            zerror!("Cannot create a new obfs link to {:?}: {}", addr, e)
        })?;

        // AEAD handshake (client role). Socket options (NODELAY/LINGER) are applied
        // afterwards in `from_stream` on the owned inner stream.
        let obfs_stream = ObfsStream::connect(tcp_stream, &psk).await.map_err(|e| {
            zerror!("obfs handshake (connect) to {:?} failed: {}", addr, e)
        })?;

        let link = Arc::new(LinkUnicastObfs::from_stream(obfs_stream, src_addr, dst_addr)?);
        Ok(LinkUnicast::from(link as Arc<dyn LinkUnicastTrait>))
    }
    // new_link:end

    // new_listener:start
//   purpose: Bind a TCP listener and spawn an accept task for incoming obfs connections.
//   input:  endpoint: EndPoint — the endpoint with bind address and config (PSK)
//   output: ZResult<Locator> — the locator of the bound listener, or error on bind
//   sideEffects: Binds TCP socket, spawns async accept task, registers listener
    async fn new_listener(&self, endpoint: EndPoint) -> ZResult<Locator> {
        let epaddr = endpoint.address();
        let epconf = endpoint.config();

        eprintln!("[obfs-new-listener] ENTER: {}", epaddr.as_str());
        let addr = get_obfs_addr(&epaddr).await?;
        let host = get_obfs_host(&epaddr)?.to_string();
        let psk = load_psk(&epconf).map_err(|e| {
            eprintln!("[obfs-new-listener] PSK load FAILED: {}", e);
            e
        })?;
        let socket_config = tcp_socket_config_from(&epconf).await?;

        // Bind the TCP listener.
        let (socket, local_addr) = socket_config
            .new_listener(&addr)
            .map_err(|e| {
                eprintln!("[obfs-new-listener] bind FAILED on {}: {}", addr, e);
                zerror!("Cannot create a new obfs listener on {}: {}", addr, e)
            })?;
        eprintln!("[obfs-new-listener] bound OK: {:?}", local_addr);
        let local_port = local_addr.port();

        let token = self.listeners.token.child_token();
        let task = {
            let token = token.clone();
            let manager = self.manager.clone();
            // Server static key is derived from the PSK once per listener.
            let server_static = Arc::new(StaticServerKey::derive_from_psk(&psk));
            async move { accept_task(socket, server_static, token, manager).await }
        };

        let locator = Locator::new(
            endpoint.protocol(),
            format!("{host}:{local_port}"),
            endpoint.metadata(),
        )?;
        let endpoint = EndPoint::new(
            locator.protocol(),
            locator.address(),
            locator.metadata(),
            endpoint.config(),
        )?;
        self.listeners
            .add_listener(endpoint, local_addr, task, token)
            .await?;

        Ok(locator)
    }
    // new_listener:end

    // del_listener:start
//   purpose: Stop and remove a TCP listener by its endpoint address.
//   input:  endpoint: &EndPoint — the endpoint identifying the listener to stop
//   output: ZResult<()> — Ok on success, error if listener not found
//   sideEffects: Cancels the accept task
    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        let epaddr = endpoint.address();
        let addr = get_obfs_addr(&epaddr).await?;
        self.listeners.del_listener(addr).await
    }
    // del_listener:end

    // get_listeners:start
//   purpose: Return all active listener endpoints.
//   input:  &self
//   output: Vec<EndPoint> — list of active listener endpoints
//   sideEffects: none
    async fn get_listeners(&self) -> Vec<EndPoint> {
        self.listeners.get_endpoints()
    }
    // get_listeners:end

    // get_locators:start
//   purpose: Return the locators of all active listeners.
//   input:  &self
//   output: Vec<Locator> — list of listener locators
//   sideEffects: none
    async fn get_locators(&self) -> Vec<Locator> {
        self.listeners.get_locators()
    }
    // get_locators:end
}

// accept_task:start
//   purpose: Accept incoming TCP connections, perform AEAD handshake, deliver established links to the Zenoh channel.
//   input:  socket: TcpListener — the bound TCP listener; server_static: Arc<StaticServerKey> — the server static key for handshake; token: CancellationToken — cancellation signal; manager: NewLinkChannelSender — channel to send new links
//   output: ZResult<()> — Ok on clean cancellation, error on local_addr failure
//   sideEffects: Accepts TCP connections, performs AEAD handshake I/O, spawns handshake tasks, sends links over channel
async fn accept_task(
    socket: TcpListener,
    server_static: Arc<StaticServerKey>,
    token: CancellationToken,
    manager: NewLinkChannelSender,
) -> ZResult<()> {
    eprintln!("[obfs-accept] TASK_STARTED");
    let src_addr = socket.local_addr().map_err(|e| {
        let e = zerror!("Can not accept obfs connections: {}", e);
        tracing::warn!("{}", e);
        eprintln!("[obfs-accept] local_addr FAILED: {}", e);
        e
    })?;
    eprintln!("[obfs-accept] LOCAL_ADDR: {:?}", src_addr);

    tracing::trace!("Ready to accept obfs connections on: {:?}", src_addr);
    eprintln!("[obfs-accept] ENTERING_LOOP");
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                eprintln!("[obfs-accept] CANCELLED — loop exit");
                break;
            }

            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                eprintln!("[obfs-accept] HEARTBEAT: alive on {:?}", src_addr);
            }

            res = socket.accept() => {
                match res {
                    Ok((tcp_stream, dst_addr)) => {
                        eprintln!("[obfs-accept] TCP connection from {:?}", dst_addr);
                        let local_addr = match tcp_stream.local_addr() {
                            Ok(sa) => sa,
                            Err(e) => {
                                tracing::debug!("Can not accept obfs connection: {}", e);
                                continue;
                            }
                        };
                        if let Err(err) = tcp_stream.set_nodelay(true) {
                            tracing::warn!("Unable to set NODELAY on accepted obfs link: {err}");
                        }

                        let server_static = server_static.clone();
                        let manager = manager.clone();
                        // Perform the AEAD handshake off the accept loop so a slow
                        // or malicious peer cannot stall other connections.
                        zenoh_runtime::ZRuntime::Acceptor.spawn(async move {
                            eprintln!("[obfs-accept] Starting handshake with {:?}", dst_addr);
                            match ObfsStream::accept(tcp_stream, &server_static).await {
                                Ok(obfs_stream) => {
                                    eprintln!("[obfs-accept] Handshake OK from {:?}", dst_addr);
                                    tracing::debug!(
                                        "Accepted obfs connection on {:?}: {:?}",
                                        local_addr, dst_addr
                                    );
                                    match LinkUnicastObfs::from_stream(
                                        obfs_stream, local_addr, dst_addr,
                                    ) {
                                        Ok(link) => {
                                            let link = Arc::new(link);
                                            if let Err(e) = manager
                                                .send_async(LinkUnicast::from(
                                                    link as Arc<dyn LinkUnicastTrait>,
                                                ))
                                                .await
                                            {
                                                tracing::error!("{}-{}: {}", file!(), line!(), e);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "obfs: failed to build link {:?} => {:?}: {}",
                                                local_addr, dst_addr, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("[obfs-accept] Handshake FAILED from {:?}: {}", dst_addr, e);
                                    tracing::debug!(
                                        "obfs handshake (accept) from {:?} failed: {}",
                                        dst_addr, e
                                    );
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("{}. Hint: increase the system open file limit.", e);
                        tokio::time::sleep(Duration::from_micros(*OBFS_ACCEPT_THROTTLE_TIME)).await;
                    }
                }
            }
        }
    }

    Ok(())
}
// accept_task:end
