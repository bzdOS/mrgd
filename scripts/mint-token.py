#!/usr/bin/env python3
"""Mint a matrix-hs access token without a password.

Purpose: the gamma login migration (the scope-separation record (kept private) Phase 4 prerequisite 1).
Phase 2 added a revocation epoch to the token payload, so every pre-upgrade
token stopped parsing after the cluster upgrade — and the NVR bot's password
is stored nowhere, so "just log in again" is not available for it. This script
re-mints a valid token directly from MATRIX_HS_TOKEN_SECRET, which we own.

Format (src/auth.rs::sign_token — the test vector is the code):
  token  = "mxt_" + b64url_nopad(payload) + "." + b64url_nopad(mac)
  payload= "<user_id>|<device_id>|<epoch>|<8 random bytes hex>"
  mac    = HMAC-SHA256(secret, b64url_nopad(payload)-as-ascii-bytes)
The secret is the RAW BYTES of the MATRIX_HS_TOKEN_SECRET string (not hex-
decoded) — see TokenSecret::global.

Epoch note: verify only checks token.epoch >= record.epoch, so minting with
epoch 0 is safe unless the record's epoch was raised (logout_devices/password
change). For a bot nobody logs out, 0 is right.

Usage:
  MATRIX_HS_TOKEN_SECRET=... ./mint-token.py -u '@nvr-alerts-bot:localhost' -d DEVICE1
  ./mint-token.py --secret-file /path/env -u ... -d ... -e 0
The env file form reads KEY=VALUE lines and takes MATRIX_HS_TOKEN_SECRET.

Verify before deploying: point the token at the server's /whoami and expect
the right user_id back. Never paste a minted token anywhere you would not
paste the secret itself.
"""
import argparse
import base64
import hashlib
import hmac
import os
import secrets
import sys


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def load_secret(args) -> bytes:
    if args.secret_file:
        val = ""
        for line in open(args.secret_file, encoding="utf-8"):
            line = line.strip()
            if line.startswith("MATRIX_HS_TOKEN_SECRET="):
                val = line.split("=", 1)[1].strip()
                break
        if not val:
            sys.exit("mint-token: no MATRIX_HS_TOKEN_SECRET= in " + args.secret_file)
    else:
        val = os.environ.get("MATRIX_HS_TOKEN_SECRET", "")
    if not val:
        sys.exit("mint-token: MATRIX_HS_TOKEN_SECRET not set (env or --secret-file)")
    return val.encode("utf-8")  # raw string bytes, per TokenSecret::global


def main() -> None:
    ap = argparse.ArgumentParser(description="mint a matrix-hs mxt_ token")
    ap.add_argument("-u", "--user", required=True, help="full user_id, e.g. '@bot:host'")
    ap.add_argument("-d", "--device", required=True, help="device_id, e.g. DEVICE1")
    ap.add_argument("-e", "--epoch", type=int, default=0, help="revocation epoch (default 0)")
    ap.add_argument("--secret-file", help="file with MATRIX_HS_TOKEN_SECRET=... (env otherwise)")
    args = ap.parse_args()

    secret = load_secret(args)
    nonce = secrets.token_hex(8)  # 8 bytes, hex — matches OsRng in sign_token
    payload = f"{args.user}|{args.device}|{args.epoch}|{nonce}"
    payload_b64 = b64u(payload.encode("utf-8"))
    mac = hmac.new(secret, payload_b64.encode("ascii"), hashlib.sha256).digest()
    print(f"mxt_{payload_b64}.{b64u(mac)}")


if __name__ == "__main__":
    main()
