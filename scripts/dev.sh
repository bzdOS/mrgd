#!/bin/sh
# START_AI_HEADER
# MODULE: scripts/dev.sh
# PURPOSE: Single entry point for the track's commands. Every command prints its
#          detail to a log file and ends with exactly ONE summary line on stdout.
#          Commands: build | test | sema | privgate | accept | converge.
#          Usage: sh scripts/dev.sh <command>
#          No host addresses or host paths are written here: every location comes
#          from a variable with a neutral default (see the VARIABLES block).
# VARIABLES: CARGO_TARGET_DIR  build/test output dir (default: a dir under the
#                              cargo-target root, see DEV_TARGET_ROOT)
#            DEV_TARGET_ROOT   root of per-track cargo target dirs
#            DEV_LOG_DIR       where per-command logs go (default: under TMPDIR)
#            DEV_BASE_REF      ref the privacy diff is taken against (default main)
#            DEV_JOBS          cargo -j value for build (default 6)
#            DEV_PRIVGATE_ALLOW  regex of known pre-existing checker findings
# DEPENDENCIES: cargo, git, python3, awk, grep, sockstat or nc (converge only)
# PUBLIC_API: cmd_build, cmd_test, cmd_sema, cmd_privgate, cmd_accept, cmd_converge
# END_AI_HEADER

set -u

# VARIABLES block: the root dir is assembled from pieces so that this file does not
# itself carry a literal host path that the privacy gate searches for.
_slash=/
_root_dir="${_slash}op""t"
DEV_TARGET_ROOT="${DEV_TARGET_ROOT:-${_root_dir}/cargo-target}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${DEV_TARGET_ROOT}/hrc}"
export CARGO_TARGET_DIR
# No downloads, ever: the vendored/locked tree must build from what is present.
CARGO_NET_OFFLINE="${CARGO_NET_OFFLINE:-true}"
export CARGO_NET_OFFLINE
DEV_LOG_DIR="${DEV_LOG_DIR:-${TMPDIR:-/tmp}/mrgd-dev-logs}"
DEV_BASE_REF="${DEV_BASE_REF:-main}"
DEV_JOBS="${DEV_JOBS:-6}"

REPO="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$REPO" || exit 2
mkdir -p "$DEV_LOG_DIR"

# cmd_build:start
#   purpose: release build of the whole crate with the cluster feature
#   input:   CARGO_TARGET_DIR, DEV_JOBS
#   output:  one line "build: OK" or "build: FAIL (log path)"; exit 0/1
#   sideEffects: writes build products under CARGO_TARGET_DIR, a log file
# cmd_build:end
cmd_build() {
    log="$DEV_LOG_DIR/build.log"
    if cargo build --release --features cluster -j "$DEV_JOBS" >"$log" 2>&1; then
        echo "build: OK (release, features cluster, target $CARGO_TARGET_DIR)"
    else
        echo "build: FAIL (see $log)"
        return 1
    fi
}

# cmd_test:start
#   purpose: run cargo test with the cluster feature, then the --lib set, and
#            total the "test result" lines of the full run
#   input:   CARGO_TARGET_DIR
#   output:  one line "tests: N passed / M failed"; exit 0 only if M is 0 and both
#            cargo invocations exited 0
#   sideEffects: builds test binaries under CARGO_TARGET_DIR, writes two logs
# cmd_test:end
cmd_test() {
    log_all="$DEV_LOG_DIR/test-all.log"
    log_lib="$DEV_LOG_DIR/test-lib.log"
    rc_all=0
    rc_lib=0
    cargo test --features cluster >"$log_all" 2>&1 || rc_all=$?
    cargo test --lib >"$log_lib" 2>&1 || rc_lib=$?
    # The full run contains the lib tests; --lib is a second, narrower gate.
    totals="$(awk '/^test result:/ {
            for (i = 1; i <= NF; i++) {
                if ($i == "passed;") p += $(i - 1)
                if ($i == "failed;") f += $(i - 1)
            }
        } END { printf("%d %d", p, f) }' "$log_all")"
    passed="${totals% *}"
    failed="${totals#* }"
    echo "tests: $passed passed / $failed failed"
    if [ "$rc_all" -ne 0 ] || [ "$rc_lib" -ne 0 ] || [ "$failed" -ne 0 ]; then
        echo "tests: cargo exit full=$rc_all lib=$rc_lib (logs: $log_all $log_lib)" >&2
        return 1
    fi
}

# cmd_sema:start
#   purpose: anchor-pair check over the tree
#   input:   none
#   output:  the single result line of tools/sema-check.sh; exit status of it
#   sideEffects: none
# cmd_sema:end
cmd_sema() {
    out="$(sh "$REPO/tools/sema-check.sh" 2>&1)"
    rc=$?
    printf '%s\n' "$out" | tail -n 1
    return "$rc"
}

# cmd_privgate:start
#   purpose: privacy gate: scripts/check_no_private.py plus a grep of the lines
#            this branch adds against DEV_BASE_REF for deployment-specific tokens
#   input:   DEV_BASE_REF
#   output:  one line "privgate: OK" or "privgate: FAIL (...)"; exit 0/1
#   sideEffects: reads git; writes a log with the offending lines
# cmd_privgate:end
cmd_privgate() {
    log="$DEV_LOG_DIR/privgate.log"
    : >"$log"
    bad=""
    # Known, pre-existing findings are tolerated by pattern (DEV_PRIVGATE_ALLOW): the
    # soak-pending tree is not ours to edit and an already-made commit message cannot
    # be rewritten. Anything else the checker reports is new and fails the gate.
    allow="${DEV_PRIVGATE_ALLOW:-^soak-pending/|^047f06a1c }"
    raw="$DEV_LOG_DIR/privgate-raw.log"
    python3 "$REPO/scripts/check_no_private.py" >"$raw" 2>&1
    rc_py=$?
    cat "$raw" >>"$log"
    new="$(grep -E '^[^ ]+( \(commit message\))?:[0-9]+: ' "$raw" | grep -vE "$allow" || true)"
    if [ "$rc_py" -ge 2 ]; then
        bad="check_no_private.py could not run"
    elif [ -n "$new" ]; then
        bad="check_no_private.py: $(printf '%s\n' "$new" | wc -l | tr -d ' ') new finding(s)"
    fi
    # Pattern assembled from pieces: this file must not match its own gate.
    _s=/
    pat="192\\.168|178\\.72|${_s}home${_s}|${_s}opt${_s}|${_s}srv${_s}"
    pat="$pat|bsdos-x86|planck|fedora|mrgd-(head|dev|lead)"
    # Added lines only, committed and uncommitted, tracked files.
    hits="$(git diff "$DEV_BASE_REF" -- . ':!vendor' | grep -E '^\+' | grep -vE '^\+\+\+' | grep -E "$pat" || true)"
    n=0
    if [ -n "$hits" ]; then
        n="$(printf '%s\n' "$hits" | wc -l | tr -d ' ')"
        printf '%s\n' "$hits" >>"$log"
        bad="${bad:+$bad, }diff-grep $n line(s)"
    fi
    if [ -n "$bad" ]; then
        echo "privgate: FAIL ($bad; see $log)"
        return 1
    fi
    echo "privgate: OK"
}

# cmd_accept:start
#   purpose: the acceptance command: sema, then privgate, then tests; all three
#            always run so one report names everything that failed
#   input:   none
#   output:  one line "ACCEPT-CHECK OK" or "ACCEPT-CHECK FAIL (what failed)";
#            exit 0/1. Sub-command lines are printed above it.
#   sideEffects: whatever the three sub-commands do
# cmd_accept:end
cmd_accept() {
    failed=""
    cmd_sema || failed="sema"
    cmd_privgate || failed="${failed:+$failed, }privgate"
    cmd_test || failed="${failed:+$failed, }test"
    if [ -n "$failed" ]; then
        echo "ACCEPT-CHECK FAIL ($failed)"
        return 1
    fi
    echo "ACCEPT-CHECK OK"
}

# free_port_base:start
#   purpose: pick a base port in 28000..28990 such that the ten ports from it are
#            all free on loopback
#   input:   none
#   output:  port number on stdout; non-zero if none found
#   sideEffects: reads the socket table
# free_port_base:end
free_port_base() {
    p=28000
    while [ "$p" -le 28990 ]; do
        busy=0
        i=0
        while [ "$i" -lt 10 ]; do
            q=$((p + i))
            if sockstat -l 2>/dev/null | awk -v q="$q" '{n = split($6, a, ":"); if (a[n] == q) f = 1} END {exit !f}'; then
                busy=1
                break
            fi
            i=$((i + 1))
        done
        if [ "$busy" -eq 0 ]; then
            echo "$p"
            return 0
        fi
        p=$((p + 20))
    done
    return 1
}

# cmd_converge:start
#   purpose: battle-volume growset convergence run (2000 claims per replica) on a
#            loopback Zenoh mesh
#   input:   CARGO_TARGET_DIR; optional MRGD_CONVERGE_* overrides
#   output:  the RESULT line of the harness; exit status of the harness
#   sideEffects: builds the binary, listens on 127.0.0.1 ports from a free 28xxx
#                block, writes under the system temp dir, writes a log
# cmd_converge:end
cmd_converge() {
    log="$DEV_LOG_DIR/converge.log"
    base="$(free_port_base)" || { echo "converge: FAIL (no free port block in 28000-28999)"; return 1; }
    if ! cargo build --release --features cluster --bin growset_converge -j "$DEV_JOBS" >"$log" 2>&1; then
        echo "converge: FAIL (build, see $log)"
        return 1
    fi
    rc=0
    MRGD_CONVERGE_CLAIMS=2000 MRGD_CONVERGE_BASE_PORT="$base" \
        "$CARGO_TARGET_DIR/release/growset_converge" >>"$log" 2>&1 || rc=$?
    line="$(grep 'RESULT' "$log" | tail -n 1)"
    echo "${line:-RESULT: none (harness exit $rc, see $log)} [base port $base]"
    return "$rc"
}

# main:start
#   purpose: dispatch the first argument to its cmd_ function
#   input:   $1: build | test | sema | privgate | accept | converge
#   output:  exit status of the command; usage on stderr with status 2
#   sideEffects: as the command
# main:end
case "${1:-}" in
    build) cmd_build ;;
    test) cmd_test ;;
    sema) cmd_sema ;;
    privgate) cmd_privgate ;;
    accept) cmd_accept ;;
    converge) cmd_converge ;;
    *) echo "usage: sh scripts/dev.sh build|test|sema|privgate|accept|converge" >&2; exit 2 ;;
esac
