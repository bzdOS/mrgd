// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/net/mod.rs
// PURPOSE: Network interface discovery and helpers for binding sockets to devices.
// INTENT: Abstracts platform-specific network interface enumeration (pnet on Unix, GetAdaptersAddresses on Windows) behind a uniform API.
// DEPENDENCIES: std::net, tokio::net, pnet_datalink (unix), winapi (windows), lazy_static, zenoh_core, zenoh_result
// PUBLIC_API: get_interface, get_multicast_interfaces, get_local_addresses, get_unicast_addresses_of_multicast_interfaces, get_unicast_addresses_of_interface, get_index_of_interface, get_interface_names_by_addr, get_ipv4_ipaddrs, get_ipv6_ipaddrs, set_bind_to_device_tcp_socket, set_bind_to_device_udp_socket
// END_AI_HEADER

//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::net::{IpAddr, Ipv6Addr};
#[cfg(target_os = "freebsd")]
use std::net::{SocketAddr, SocketAddrV6};

#[cfg(unix)]
use lazy_static::lazy_static;
#[cfg(unix)]
use pnet_datalink::NetworkInterface;
// Unconditional: FreeBSD was excluded here when this crate carried no
// set_bind_to_device_* definitions for it. It has them now (below), and they
// need these types like every other platform's do.
use tokio::net::{TcpSocket, UdpSocket};
use zenoh_core::zconfigurable;
#[cfg(unix)]
use zenoh_result::zerror;
use zenoh_result::{bail, ZResult};

zconfigurable! {
    static ref WINDOWS_GET_ADAPTERS_ADDRESSES_BUF_SIZE: u32 = 8192;
    static ref WINDOWS_GET_ADAPTERS_ADDRESSES_MAX_RETRIES: u32 = 3;
}

#[cfg(unix)]
lazy_static! {
    static ref IFACES: Vec<NetworkInterface> = pnet_datalink::interfaces();
}

#[cfg(windows)]
/// # Safety
/// The caller must ensure the `af_spec`` is valid, which will be used by
/// `winapi::um::iphlpapi::GetAdaptersAddresses`.
unsafe fn get_adapters_addresses(af_spec: i32) -> ZResult<Vec<u8>> {
    use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

    let mut ret;
    let mut retries = 0;
    let mut size: u32 = *WINDOWS_GET_ADAPTERS_ADDRESSES_BUF_SIZE;
    let mut buffer: Vec<u8>;
    loop {
        buffer = Vec::with_capacity(size as usize);
        // SAFETY: Call the unsafe function `GetAdaptersAddresses`.
        ret = unsafe {
            winapi::um::iphlpapi::GetAdaptersAddresses(
                af_spec.try_into().unwrap(),
                0,
                std::ptr::null_mut(),
                buffer.as_mut_ptr() as *mut IP_ADAPTER_ADDRESSES_LH,
                &mut size,
            )
        };
        if ret != winapi::shared::winerror::ERROR_BUFFER_OVERFLOW {
            break;
        }
        if retries >= *WINDOWS_GET_ADAPTERS_ADDRESSES_MAX_RETRIES {
            break;
        }
        retries += 1;
    }

    if ret != 0 {
        bail!("GetAdaptersAddresses returned {}", ret)
    }

    Ok(buffer)
}
// get_interface:start
//   purpose: Find the first IPv4 address of a network interface by name or IP string.
//   input:  name - interface name (e.g. "eth0") or an IP address string.
//   output: ZResult<Option<IpAddr>> - None if interface not found; Err on Windows API failure.
//   sideEffects: reads cached interface list (unix) or calls GetAdaptersAddresses (windows)
pub fn get_interface(name: &str) -> ZResult<Option<IpAddr>> {
    #[cfg(unix)]
    {
        for iface in IFACES.iter() {
            if iface.name == name {
                for ifaddr in &iface.ips {
                    if ifaddr.is_ipv4() {
                        return Ok(Some(ifaddr.ip()));
                    }
                }
            }
            for ifaddr in &iface.ips {
                if ifaddr.ip().to_string() == name {
                    return Ok(Some(ifaddr.ip()));
                }
            }
        }
        Ok(None)
    }

    #[cfg(windows)]
    {
        unsafe {
            use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

            use crate::ffi;

            let buffer = get_adapters_addresses(winapi::shared::ws2def::AF_INET)?;

            let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
            while let Some(iface) = next_iface {
                if name == ffi::pstr_to_string(iface.AdapterName)
                    || name == ffi::pwstr_to_string(iface.FriendlyName)
                    || name == ffi::pwstr_to_string(iface.Description)
                {
                    let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                    while let Some(ucast_addr) = next_ucast_addr {
                        if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                            if ifaddr.is_ipv4() {
                                return Ok(Some(ifaddr.ip()));
                            }
                        }
                        next_ucast_addr = ucast_addr.Next.as_ref();
                    }
                }

                let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                while let Some(ucast_addr) = next_ucast_addr {
                    if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                        if ifaddr.ip().to_string() == name {
                            return Ok(Some(ifaddr.ip()));
                        }
                    }
                    next_ucast_addr = ucast_addr.Next.as_ref();
                }
                next_iface = iface.Next.as_ref();
            }
            Ok(None)
        }
    }
}
// get_interface:end

/// Get the network interface to bind the UDP sending port to when not specified by user
// get_multicast_interfaces:start
//   purpose: Return IPv4 addresses of all up, running, multicast-capable interfaces.
//   input:  none.
//   output: Vec<IpAddr> - IPv4 addresses of multicast interfaces; empty on Windows.
//   sideEffects: reads cached interface list (unix)
pub fn get_multicast_interfaces() -> Vec<IpAddr> {
    #[cfg(unix)]
    {
        IFACES
            .iter()
            .filter_map(|iface| {
                if iface.is_up() && iface.is_running() && iface.is_multicast() {
                    for ipaddr in &iface.ips {
                        if ipaddr.is_ipv4() {
                            return Some(ipaddr.ip());
                        }
                    }
                }
                None
            })
            .collect()
    }
    #[cfg(windows)]
    {
        // On windows, bind to [::], the system will select the default interface
        vec![IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)]
    }
}
// get_multicast_interfaces:end

// get_local_addresses:start
//   purpose: Return all IP addresses of up/running interfaces, optionally filtered by interface name.
//   input:  interface - optional interface name to filter by; None returns all.
//   output: ZResult<Vec<IpAddr>> - list of IP addresses; Err on Windows API failure.
//   sideEffects: reads cached interface list (unix) or calls GetAdaptersAddresses (windows)
pub fn get_local_addresses(interface: Option<&str>) -> ZResult<Vec<IpAddr>> {
    #[cfg(unix)]
    {
        Ok(IFACES
            .iter()
            .filter(|iface| {
                if let Some(interface) = interface.as_ref() {
                    if iface.name != *interface {
                        return false;
                    }
                }
                iface.is_up() && iface.is_running()
            })
            .flat_map(|iface| iface.ips.clone())
            .map(|ipnet| ipnet.ip())
            .collect())
    }

    #[cfg(windows)]
    {
        unsafe {
            use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

            use crate::ffi;

            let buffer = get_adapters_addresses(winapi::shared::ws2def::AF_UNSPEC)?;

            let mut result = vec![];
            let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
            while let Some(iface) = next_iface {
                if let Some(interface) = interface.as_ref() {
                    if ffi::pstr_to_string(iface.AdapterName) != *interface {
                        continue;
                    }
                }
                let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                while let Some(ucast_addr) = next_ucast_addr {
                    if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                        result.push(ifaddr.ip());
                    }
                    next_ucast_addr = ucast_addr.Next.as_ref();
                }
                next_iface = iface.Next.as_ref();
            }
            Ok(result)
        }
    }
}
// get_local_addresses:end

/// Get the network interface to bind the UDP sending port to when not specified by user
// get_unicast_addresses_of_multicast_interfaces:start
//   purpose: Return non-multicast IP addresses of all up, running, multicast-capable interfaces.
//   input:  none.
//   output: Vec<IpAddr> - unicast IPs of multicast interfaces; empty on Windows.
//   sideEffects: reads cached interface list (unix)
pub fn get_unicast_addresses_of_multicast_interfaces() -> Vec<IpAddr> {
    #[cfg(unix)]
    {
        IFACES
            .iter()
            .filter(|iface| iface.is_up() && iface.is_running() && iface.is_multicast())
            .flat_map(|iface| {
                iface
                    .ips
                    .iter()
                    .filter(|ip| !ip.ip().is_multicast())
                    .map(|x| x.ip())
                    .collect::<Vec<IpAddr>>()
            })
            .collect()
    }
    #[cfg(windows)]
    {
        // On windows, bind to [::] or [::], the system will select the default interface
        vec![]
    }
}
// get_unicast_addresses_of_multicast_interfaces:end

// get_unicast_addresses_of_interface:start
//   purpose: Return non-multicast IP addresses of a specific named interface, validating it is up and running.
//   input:  name - interface name to query.
//   output: ZResult<Vec<IpAddr>> - Err if interface not found, not up, or not running.
//   sideEffects: reads cached interface list (unix) or calls GetAdaptersAddresses (windows)
pub fn get_unicast_addresses_of_interface(name: &str) -> ZResult<Vec<IpAddr>> {
    #[cfg(unix)]
    {
        match IFACES.iter().find(|iface| iface.name == name) {
            Some(iface) => {
                if !iface.is_up() {
                    bail!("Interface {name} is not up");
                }
                if !iface.is_running() {
                    bail!("Interface {name} is not running");
                }
                let addrs = iface
                    .ips
                    .iter()
                    .filter(|ip| !ip.ip().is_multicast())
                    .map(|x| x.ip())
                    .collect::<Vec<IpAddr>>();
                Ok(addrs)
            }
            None => bail!("Interface {name} not found"),
        }
    }

    #[cfg(windows)]
    {
        unsafe {
            use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

            use crate::ffi;

            let buffer = get_adapters_addresses(winapi::shared::ws2def::AF_INET)?;

            let mut addrs = vec![];
            let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
            while let Some(iface) = next_iface {
                if name == ffi::pstr_to_string(iface.AdapterName)
                    || name == ffi::pwstr_to_string(iface.FriendlyName)
                    || name == ffi::pwstr_to_string(iface.Description)
                {
                    let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                    while let Some(ucast_addr) = next_ucast_addr {
                        if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                            addrs.push(ifaddr.ip());
                        }
                        next_ucast_addr = ucast_addr.Next.as_ref();
                    }
                }
                next_iface = iface.Next.as_ref();
            }
            Ok(addrs)
        }
    }
}
// get_unicast_addresses_of_interface:end

// get_index_of_interface:start
//   purpose: Find the interface index for the interface that has the given IP address.
//   input:  addr - IP address to search for.
//   output: ZResult<u32> - interface index (IPv6 interface index on Windows); Err if no matching interface found.
//   sideEffects: reads cached interface list (unix) or calls GetAdaptersAddresses (windows)
pub fn get_index_of_interface(addr: IpAddr) -> ZResult<u32> {
    #[cfg(unix)]
    {
        IFACES
            .iter()
            .find(|iface| iface.ips.iter().any(|ipnet| ipnet.ip() == addr))
            .map(|iface| iface.index)
            .ok_or_else(|| zerror!("No interface found with address {addr}").into())
    }
    #[cfg(windows)]
    {
        unsafe {
            use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

            use crate::ffi;

            let buffer = get_adapters_addresses(winapi::shared::ws2def::AF_INET)?;

            let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
            while let Some(iface) = next_iface {
                let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                while let Some(ucast_addr) = next_ucast_addr {
                    if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                        if ifaddr.ip() == addr {
                            return Ok(iface.Ipv6IfIndex);
                        }
                    }
                    next_ucast_addr = ucast_addr.Next.as_ref();
                }
                next_iface = iface.Next.as_ref();
            }
            bail!("No interface found with address {addr}")
        }
    }
}
// get_index_of_interface:end

// get_interface_names_by_addr:start
//   purpose: Return names of all interfaces that have the given IP address (or all interfaces if addr is unspecified).
//   input:  addr - IP address to match, or unspecified to return all interfaces.
//   output: ZResult<Vec<String>> - list of interface names; Err on Windows API failure.
//   sideEffects: reads cached interface list (unix) or calls GetAdaptersAddresses (windows)
pub fn get_interface_names_by_addr(addr: IpAddr) -> ZResult<Vec<String>> {
    #[cfg(unix)]
    {
        if addr.is_unspecified() {
            Ok(IFACES
                .iter()
                .map(|iface| iface.name.clone())
                .collect::<Vec<String>>())
        } else {
            let addr = addr.to_canonical();
            Ok(IFACES
                .iter()
                .filter(|iface| iface.ips.iter().any(|ipnet| ipnet.ip() == addr))
                .map(|iface| iface.name.clone())
                .collect::<Vec<String>>())
        }
    }
    #[cfg(windows)]
    {
        let mut result = vec![];
        unsafe {
            use winapi::um::iptypes::IP_ADAPTER_ADDRESSES_LH;

            use crate::ffi;

            let buffer = get_adapters_addresses(winapi::shared::ws2def::AF_UNSPEC)?;

            if addr.is_unspecified() {
                let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
                while let Some(iface) = next_iface {
                    result.push(ffi::pstr_to_string(iface.AdapterName));
                    next_iface = iface.Next.as_ref();
                }
            } else {
                let addr = addr.to_canonical();
                let mut next_iface = (buffer.as_ptr() as *mut IP_ADAPTER_ADDRESSES_LH).as_ref();
                while let Some(iface) = next_iface {
                    let mut next_ucast_addr = iface.FirstUnicastAddress.as_ref();
                    while let Some(ucast_addr) = next_ucast_addr {
                        if let Ok(ifaddr) = ffi::win::sockaddr_to_addr(ucast_addr.Address) {
                            if ifaddr.ip() == addr {
                                result.push(ffi::pstr_to_string(iface.AdapterName));
                            }
                        }
                        next_ucast_addr = ucast_addr.Next.as_ref();
                    }
                    next_iface = iface.Next.as_ref();
                }
            }
        }
        Ok(result)
    }
}
// get_interface_names_by_addr:end

// get_ipv4_ipaddrs:start
//   purpose: Return non-loopback, non-multicast IPv4 addresses of an interface (or all if None).
//   input:  interface - optional interface name filter.
//   output: Vec<IpAddr> - filtered IPv4 addresses; empty on lookup error.
//   sideEffects: calls get_local_addresses
pub fn get_ipv4_ipaddrs(interface: Option<&str>) -> Vec<IpAddr> {
    get_local_addresses(interface)
        .unwrap_or_else(|_| vec![])
        .drain(..)
        .filter_map(|x| match x {
            IpAddr::V4(a) => Some(a),
            IpAddr::V6(_) => None,
        })
        .filter(|x| !x.is_loopback() && !x.is_multicast())
        .map(IpAddr::V4)
        .collect()
}
// get_ipv4_ipaddrs:end

// get_ipv6_ipaddrs:start
//   purpose: Return sorted IP addresses preferring non-linklocal IPv6, then public IPv4, then link-local IPv6, then private IPv4.
//   input:  interface - optional interface name filter.
//   output: Vec<IpAddr> - prioritized list of addresses; empty on lookup error.
//   sideEffects: calls get_local_addresses
pub fn get_ipv6_ipaddrs(interface: Option<&str>) -> Vec<IpAddr> {
    const fn is_unicast_link_local(addr: &Ipv6Addr) -> bool {
        (addr.segments()[0] & 0xffc0) == 0xfe80
    }

    let ipaddrs = get_local_addresses(interface).unwrap_or_else(|_| vec![]);

    // Get first all IPv4 addresses
    let ipv4_iter = ipaddrs
        .iter()
        .filter_map(|x| match x {
            IpAddr::V4(a) => Some(a),
            IpAddr::V6(_) => None,
        })
        .filter(|x| {
            !x.is_loopback() && !x.is_link_local() && !x.is_multicast() && !x.is_broadcast()
        });

    // Get next all IPv6 addresses
    let ipv6_iter = ipaddrs.iter().filter_map(|x| match x {
        IpAddr::V4(_) => None,
        IpAddr::V6(a) => Some(a),
    });

    // First match non-linklocal IPv6 addresses
    let nll_ipv6_addrs = ipv6_iter
        .clone()
        .filter(|x| !x.is_loopback() && !x.is_multicast() && !is_unicast_link_local(x))
        .map(|x| IpAddr::V6(*x));

    // Second match public IPv4 addresses
    let pub_ipv4_addrs = ipv4_iter
        .clone()
        .filter(|x| !x.is_private())
        .map(|x| IpAddr::V4(*x));

    // Third match linklocal IPv6 addresses
    let yll_ipv6_addrs = ipv6_iter
        .filter(|x| !x.is_loopback() && !x.is_multicast() && is_unicast_link_local(x))
        .map(|x| IpAddr::V6(*x));

    // Fourth match private IPv4 addresses
    let priv_ipv4_addrs = ipv4_iter
        .clone()
        .filter(|x| x.is_private())
        .map(|x| IpAddr::V4(*x));

    // Extend
    nll_ipv6_addrs
        .chain(pub_ipv4_addrs)
        .chain(yll_ipv6_addrs)
        .chain(priv_ipv4_addrs)
        .collect()
}
// get_ipv6_ipaddrs:end

#[cfg(any(target_os = "linux", target_os = "android"))]
// set_bind_to_device_tcp_socket:start
//   purpose: Bind a TCP socket to a specific network interface (Linux/Android SO_BINDTODEVICE).
//   input:  socket - the TcpSocket to bind; iface - interface name string.
//   output: ZResult<()> - Err if bind_device fails.
//   sideEffects: sets SO_BINDTODEVICE on the socket via bind_device
pub fn set_bind_to_device_tcp_socket(socket: &TcpSocket, iface: &str) -> ZResult<()> {
    socket.bind_device(Some(iface.as_bytes()))?;
    Ok(())
}
// set_bind_to_device_tcp_socket:end

#[cfg(any(target_os = "linux", target_os = "android"))]
// set_bind_to_device_udp_socket:start
//   purpose: Bind a UDP socket to a specific network interface (Linux/Android SO_BINDTODEVICE).
//   input:  socket - the UdpSocket to bind; iface - interface name string.
//   output: ZResult<()> - Err if bind_device fails.
//   sideEffects: sets SO_BINDTODEVICE on the socket via bind_device
pub fn set_bind_to_device_udp_socket(socket: &UdpSocket, iface: &str) -> ZResult<()> {
    socket.bind_device(Some(iface.as_bytes()))?;
    Ok(())
}
// set_bind_to_device_udp_socket:end

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "windows"))]
// set_bind_to_device_tcp_socket:start
//   purpose: Warn that TCP socket-to-device binding is not supported on macOS/iOS/Windows (no-op).
//   input:  socket - the TcpSocket; iface - interface name (ignored).
//   output: ZResult<()> - always Ok(()).
//   sideEffects: emits tracing::warn log message
pub fn set_bind_to_device_tcp_socket(socket: &TcpSocket, iface: &str) -> ZResult<()> {
    tracing::warn!("Binding the socket {socket:?} to the interface {iface} is not supported on macOS, iOS, and Windows");
    Ok(())
}
// set_bind_to_device_tcp_socket:end

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "windows"))]
// set_bind_to_device_udp_socket:start
//   purpose: Warn that UDP socket-to-device binding is not supported on macOS/iOS/Windows (no-op).
//   input:  socket - the UdpSocket; iface - interface name (ignored).
//   output: ZResult<()> - always Ok(()).
//   sideEffects: emits tracing::warn log message
pub fn set_bind_to_device_udp_socket(socket: &UdpSocket, iface: &str) -> ZResult<()> {
    tracing::warn!("Binding the socket {socket:?} to the interface {iface} is not supported on macOS, iOS, and Windows");
    Ok(())
}
// set_bind_to_device_udp_socket:end

// FreeBSD was left ungated upstream: zenoh-util 1.9.0 defines these two only for
// linux/android and for macos/ios/windows, so on FreeBSD zenoh-link-commons fails
// to find them (E0425) and nothing downstream builds. These are the stubs from
// freebsd-upstream.patch, which sat next to this crate unapplied — same shape as
// the macOS/Windows no-ops, which is correct here because this deployment is
// peer-to-peer and never binds a socket to a named interface.

#[cfg(target_os = "freebsd")]
// set_bind_to_device_tcp_socket:start
//   purpose: Warn that TCP socket-to-device binding is not supported on FreeBSD (no-op).
//   input:  socket - the TcpSocket; iface - interface name (ignored).
//   output: ZResult<()> - always Ok(()).
//   sideEffects: emits tracing::warn log message
pub fn set_bind_to_device_tcp_socket(socket: &TcpSocket, iface: &str) -> ZResult<()> {
    tracing::warn!("Binding the socket {socket:?} to the interface {iface} is not supported on FreeBSD");
    Ok(())
}
// set_bind_to_device_tcp_socket:end

#[cfg(target_os = "freebsd")]
// set_bind_to_device_udp_socket:start
//   purpose: Warn that UDP socket-to-device binding is not supported on FreeBSD (no-op).
//   input:  socket - the UdpSocket; iface - interface name (ignored).
//   output: ZResult<()> - always Ok(()).
//   sideEffects: emits tracing::warn log message
pub fn set_bind_to_device_udp_socket(socket: &UdpSocket, iface: &str) -> ZResult<()> {
    tracing::warn!("Binding the socket {socket:?} to the interface {iface} is not supported on FreeBSD");
    Ok(())
}
// set_bind_to_device_udp_socket:end

#[cfg(target_os = "freebsd")]
// is_link_local:start
//   purpose: Report whether an address is link-local, which needs special handling when bound.
//   input:  ip - the address to classify.
//   output: bool - true for 169.254.0.0/16 and fe80::/10.
//   sideEffects: none
fn is_link_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_link_local(),
        // Ipv6Addr::is_unicast_link_local is still unstable.
        IpAddr::V6(ip) => (ip.segments()[0] & 0xffc0) == 0xfe80,
    }
}
// is_link_local:end

#[cfg(target_os = "freebsd")]
// index_of_interface:start
//   purpose: Look up an interface's kernel index, needed as the zone id of a link-local address.
//   input:  name - interface name.
//   output: ZResult<u32> - the interface index; Err if no such interface.
//   sideEffects: reads cached interface list
fn index_of_interface(name: &str) -> ZResult<u32> {
    match IFACES.iter().find(|iface| iface.name == name) {
        Some(iface) => Ok(iface.index),
        None => bail!("Interface {name} not found"),
    }
}
// index_of_interface:end

#[cfg(target_os = "freebsd")]
// resolve_bind_addr_for_interface:start
//   purpose: Resolve the local address to bind on in order to restrict a socket to a specific
//     interface. FreeBSD has no SO_BINDTODEVICE/IP_BOUND_IF equivalent (verified against FreeBSD
//     15.1 headers: no such sockopt in netinet/in.h, netinet6/in6.h or sys/socket.h), so the
//     interface is selected by binding one of its own addresses instead of a device-level sockopt.
//   input:  iface - interface name; addr - address the caller intends to bind/connect with.
//   output: ZResult<SocketAddr> - addr unchanged if its IP is concrete and belongs to iface;
//     otherwise iface's own address of the same family, preferring routable over link-local.
//     Err if the concrete IP is not on iface, or iface has no address of that family.
//   sideEffects: reads cached interface list
//   note: callers must use this to compute the bind address BEFORE their single bind(2) call
//     (a socket can only be bound once) — there is no set_bind_to_device_{tcp,udp}_socket for
//     FreeBSD to call after an existing bind, unlike Linux/macOS/iOS/Windows.
//   note: the interface list is a process-lifetime snapshot, so an address assigned to iface
//     after startup is not visible here.
pub fn resolve_bind_addr_for_interface(iface: &str, addr: SocketAddr) -> ZResult<SocketAddr> {
    let addrs = get_unicast_addresses_of_interface(iface)?;

    // A concrete address is validated, never rewritten: silently binding somewhere the caller
    // did not ask for would hide the misconfiguration instead of reporting it.
    if !addr.ip().is_unspecified() {
        return if addrs.contains(&addr.ip()) {
            Ok(addr)
        } else {
            bail!(
                "Cannot bind to {} on interface {iface}: the interface has no such address",
                addr.ip()
            )
        };
    }

    let want_v6 = addr.is_ipv6();
    let (mut routable, mut link_local) = (None, None);
    for ip in addrs {
        if ip.is_ipv6() != want_v6 || ip.is_loopback() {
            continue;
        }
        if is_link_local(&ip) {
            link_local.get_or_insert(ip);
        } else if routable.is_none() {
            routable = Some(ip);
        }
    }

    match routable.or(link_local) {
        // A link-local IPv6 address cannot be bound without its zone index — FreeBSD answers
        // EADDRNOTAVAIL — and the interface list does not carry one, so take it from the
        // interface index.
        Some(IpAddr::V6(ip)) if is_link_local(&IpAddr::V6(ip)) => Ok(SocketAddr::V6(
            SocketAddrV6::new(ip, addr.port(), 0, index_of_interface(iface)?),
        )),
        Some(ip) => Ok(SocketAddr::new(ip, addr.port())),
        None => bail!(
            "Interface {iface} has no {} address to bind to",
            if want_v6 { "IPv6" } else { "IPv4" }
        ),
    }
}
// resolve_bind_addr_for_interface:end
