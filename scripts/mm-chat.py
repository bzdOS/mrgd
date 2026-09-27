#!/usr/bin/env python3
"""mm-chat — tiny terminal client for watching multi-master convergence live.

Speaks only the CS-API endpoints needed to prove replication works: login,
createRoom, send, sync. Point two instances at two nodes of the same cluster,
type in one, watch it appear in the other — the events cross via Zenoh, never
over the CS-API.

Stdlib only, no dependencies, so it runs anywhere the node runs.

Usage:
    python3 scripts/mm-chat.py http://127.0.0.1:8448 alice
    python3 scripts/mm-chat.py http://<other-node>:8448 bob   [room_alias]

Both instances must pass the SAME room alias (default "demo-room"). With an
alias, createRoom is deterministic — every node derives the same
"!<alias>:<server>" — which is what lets two independently-created rooms be the
same room. Without an alias each node mints a globally-unique id instead
(see routes/rooms.rs), and the two clients would sit in different rooms.

Type a line + Enter to send. Ctrl-C (or Ctrl-D) to quit.
"""
import sys, json, time, threading, itertools, os, urllib.request


def req(method, url, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Content-Type", "application/json")
    if token:
        r.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.load(resp)


def main():
    if len(sys.argv) < 3:
        print("usage: mm-chat.py http://HOST:8448 USERNAME [room_alias]")
        sys.exit(1)
    base = sys.argv[1].rstrip("/")
    user = sys.argv[2]
    alias = sys.argv[3] if len(sys.argv) > 3 else "demo-room"

    login = req("POST", f"{base}/_matrix/client/v3/login",
                body={"type": "m.login.password",
                      "identifier": {"type": "m.id.user", "user": user},
                      "password": "x"})
    token, me = login["access_token"], login["user_id"]
    room = req("POST", f"{base}/_matrix/client/v3/createRoom", token,
               {"room_alias_name": alias})["room_id"]
    print(f"* connected {base} as {me} in {room}")
    print("* type a message + Enter to send; Ctrl-C / Ctrl-D to quit\n")

    seen = set()

    def poll():
        while True:
            try:
                s = req("GET", f"{base}/_matrix/client/v3/sync", token)
                evs = (s.get("rooms", {}).get("join", {})
                        .get(room, {}).get("timeline", {}).get("events", []))
                for e in evs:
                    eid = e.get("event_id")
                    if not eid or eid in seen:
                        continue
                    seen.add(eid)
                    body = e.get("content", {}).get("body", "")
                    sender = e.get("sender", "?")
                    tag = "you" if sender == me else sender
                    print(f"  [{tag}] {body}")
            except Exception:
                pass
            time.sleep(1)

    threading.Thread(target=poll, daemon=True).start()

    txn = itertools.count(int(time.time()))
    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not line:
                continue
            t = f"m{os.getpid()}-{next(txn)}"
            req("PUT",
                f"{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/{t}",
                token, {"msgtype": "m.text", "body": line})
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
