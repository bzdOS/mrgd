#!/usr/bin/env python3
"""Decode the published east-west test vector using ONLY docs/WIRE.md.

This is the executable form of WIRE.md §12 "Conformance". It was written from
the specification text with the Rust source closed, which is the only way to
find out whether the document is actually sufficient to implement against — a
spec checked against the implementation that produced it proves nothing.

It needs no network and no Zenoh: the vector in §13 is a delta exactly as it
would appear on <prefix>/<room_id>/events. Standard library only, except that
the ed25519 check is skipped if `cryptography` is not installed.

    python3 scripts/wire_conformance.py

Exit status 0 means the document is sufficient. If the Rust test
`golden_vector_matches_the_spec` changes, this vector and WIRE.md §13 must
change with it, in the same commit — all three are one contract.
"""
import base64
import hashlib
import struct
import sys

# docs/WIRE.md §13. One signed PDU, deliberately unsorted prev_events, GC
# watermark 2.
VECTOR_HEX = (
    "010000002c00243073796e326c676553583067332d464b35334738336d5758695441494e"
    "714e5f5a78396f3263507a3178770c0021726f6f6d3a6e6f64652d610d0040616c696365"
    "3a6e6f64652d610e006d2e726f6f6d2e6d6573736167650d0000007b22626f6479223a22"
    "6869227d02000400247a7a7a04002461616104000000000000000068e5cf8b0100000600"
    "6e6f64652d61400096b0b13b2e99a1964ce38276b5be738cf30c761f9750743426921e3c"
    "43cdadc9eeaa3c288b35708a0d17728a58b86da91190ca95f9643d9e9c7481ce71f10008"
    "0200000000000000"
)
EXPECTED_EVENT_ID = "$0syn2lgeSX0g3-FK53G83mWXiTAINqN_Zx9o2cPz1xw"
SIGNER_PUBKEY_HEX = "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c"


class Reader:
    """§4 framing primitives.

    Every read is bounds-checked because §4 requires a decoder to be total:
    these bytes arrive from the network and are decoded BEFORE any signature is
    verified, so a malformed payload must become "not a message", never a crash.
    """

    def __init__(self, buf):
        self.buf, self.off = buf, 0

    def take(self, n):
        if n < 0 or self.off + n > len(self.buf):
            raise ValueError("truncated")
        b = self.buf[self.off:self.off + n]
        self.off += n
        return b

    def u16(self):
        return struct.unpack_from("<H", self.take(2))[0]

    def u32(self):
        return struct.unpack_from("<I", self.take(4))[0]

    def u64(self):
        return struct.unpack_from("<Q", self.take(8))[0]

    def str16(self):
        return self.take(self.u16()).decode("utf-8")

    def bin16(self):
        return self.take(self.u16())

    def bin32(self):
        return self.take(self.u32())

    def remaining(self):
        return len(self.buf) - self.off


def decode_delta(buf):
    """§6 — transport framing of a RoomLogDelta."""
    r = Reader(buf)
    pdus = []
    for _ in range(r.u32()):
        pdu = {
            "event_id": r.str16(),
            "room_id": r.str16(),
            "sender": r.str16(),
            "kind": r.str16(),
            "content": r.bin32(),
        }
        pdu["prev_events"] = [r.str16() for _ in range(r.u16())]
        pdu["depth"] = r.u64()
        pdu["ts"] = r.u64()
        # §11.5 — signer_node precedes sig on the wire.
        pdu["signer_node"] = r.str16()
        pdu["sig"] = r.bin16()
        pdus.append(pdu)
    # §6.1 — an absent trailing watermark reads as 0, not as a parse error.
    return {"pdus": pdus, "collected_depth": r.u64() if r.remaining() >= 8 else 0}


def canonical_bytes(pdu):
    """§5.1.

    Two traps live here. Every length is u64-LE, not the u16/u32 the transport
    uses for the same fields (§11.4); and prev_events are sorted before being
    written, so the order the sender happened to hold them in does not change
    the result.
    """
    out = bytearray()

    def field(b):
        out.extend(struct.pack("<Q", len(b)))
        out.extend(b)

    field(pdu["room_id"].encode())
    field(pdu["sender"].encode())
    field(pdu["kind"].encode())
    field(pdu["content"])
    prevs = sorted(p.encode() for p in pdu["prev_events"])
    out.extend(struct.pack("<Q", len(prevs)))
    for p in prevs:
        field(p)
    out.extend(struct.pack("<Q", pdu["depth"]))
    out.extend(struct.pack("<Q", pdu["ts"]))
    return bytes(out)


def compute_event_id(pdu):
    """§5.2 — the content address."""
    digest = hashlib.sha256(canonical_bytes(pdu)).digest()
    return "$" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def domain_of(sender):
    """§5.4 — everything after the FIRST colon; no colon is a reject."""
    _, sep, rest = sender.partition(":")
    return rest if sep else None


def main():
    failures = []

    def check(label, got, want):
        if got == want:
            print(f"  ok    {label}: {got!r}")
        else:
            print(f"  FAIL  {label}: {got!r} (expected {want!r})")
            failures.append(label)

    delta = decode_delta(bytes.fromhex(VECTOR_HEX))
    pdu = delta["pdus"][0]

    print("§6   transport framing")
    check("pdu count", len(delta["pdus"]), 1)
    check("collected_depth", delta["collected_depth"], 2)
    check("room_id", pdu["room_id"], "!room:node-a")
    check("sender", pdu["sender"], "@alice:node-a")
    check("kind", pdu["kind"], "m.room.message")
    check("content", pdu["content"], b'{"body":"hi"}')
    check("prev_events stay unsorted on the wire", pdu["prev_events"], ["$zzz", "$aaa"])
    check("depth", pdu["depth"], 4)
    check("ts", pdu["ts"], 1700000000000)
    check("signer_node", pdu["signer_node"], "node-a")
    check("sig length", len(pdu["sig"]), 64)

    print("\n§5.1/§5.2  canonical bytes and content address")
    check("event_id recomputed independently", compute_event_id(pdu), EXPECTED_EVENT_ID)
    check("id on the wire agrees", pdu["event_id"], EXPECTED_EVENT_ID)

    print("\n§5.4  sender-domain binding")
    check("domain(sender) == signer_node", domain_of(pdu["sender"]), pdu["signer_node"])

    print("\n§5.3  ed25519 over the same canonical bytes")
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ImportError:
        print("  skip  no ed25519 library available (pip install cryptography)")
    else:
        vk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(SIGNER_PUBKEY_HEX))
        try:
            vk.verify(pdu["sig"], canonical_bytes(pdu))
            check("signature verifies", True, True)
        except Exception as exc:  # noqa: BLE001 - any failure is a failure
            check("signature verifies", f"rejected: {exc}", True)

    print("\n§4   decoder totality")
    malformed = [
        b"",                                    # empty
        b"\x01",                                # shorter than the count
        b"\x01\x00\x00\x00",                    # count, then nothing
        b"\x01\x00\x00\x00\x00",                # the five-byte case
        b"\xff\xff\xff\xff\x00",                # absurd count
        bytes.fromhex(VECTOR_HEX)[:20],         # truncated mid-PDU
        b"\x01\x00\x00\x00\xff\xff" + b"short",  # length prefix past the end
        b"\x01\x00\x00\x00\x02\x00\xff\xfe",    # invalid UTF-8
    ]
    for i, blob in enumerate(malformed):
        try:
            decode_delta(blob)
        except (ValueError, struct.error, UnicodeDecodeError):
            continue
        print(f"  FAIL  malformed #{i} parsed instead of being rejected")
        failures.append(f"malformed #{i}")
    else:
        if not failures:
            print(f"  ok    all {len(malformed)} malformed inputs rejected, no crash")

    if failures:
        print(f"\nFAIL — {len(failures)} check(s) failed; docs/WIRE.md has a gap")
        return 1
    print("\nPASS — docs/WIRE.md is sufficient to decode the wire")
    return 0


if __name__ == "__main__":
    sys.exit(main())
