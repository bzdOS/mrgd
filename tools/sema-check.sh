#!/bin/sh
# tools/sema-check.sh
#
# Header marker and contract-pair checker (POSIX sh + POSIX awk; runs on the
# FreeBSD /usr/bin/awk, no gawk-only features).
#
# Contract (sema, section 6.6):
# purpose: verify that every anchor opened in a source file is closed in the
#          same file, and that no anchor is opened twice.
# input:   repo root = parent of tools/; files *.rs *.zig *.sh *.c *.h *.cpp
#          *.hpp *.qml Makefile; pruned: target .git soak-pending vendor
#          (and the reference-script leftovers .virtfs_metadata artefacts).
# output:  exit 0 and "sema-check: OK"; otherwise per-file findings on stdout
#          and exit 1 with "sema-check: anchor validation failed" on stderr.
# sideEffects: creates and removes one temp file list; reads the tree only.
#
# Note for the awk part: the per-file counters are reset when FILENAME
# changes, and the last file is checked in the END rule (ENDFILE is gawk).

set -eu

# sema-check:root:start
#   purpose: resolve the repo root as the parent of tools/
#   input:   $0
#   output:  ROOT
#   sideEffects: none
# sema-check:root:end
ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"

# sema-check:fail:start
#   purpose: print a message to stderr and stop with status 1
#   input:   $*: message
#   output:  none (exits)
#   sideEffects: writes stderr, terminates the script
# sema-check:fail:end
fail() {
    echo "sema-check: $*" >&2
    exit 1
}

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# sema-check:collect:start
#   purpose: list candidate source files, skipping build output, git data,
#            pending soak artefacts and vendored trees
#   input:   ROOT
#   output:  file list in $tmp
#   sideEffects: writes $tmp
# sema-check:collect:end
find "$ROOT" \
    \( -path "$ROOT/.git" -o -path "$ROOT/target" -o -path "$ROOT/soak-pending" -o -path "$ROOT/vendor" -o -path "$ROOT/.virtfs_metadata" -o -path "$ROOT/artefacts" -o -path '*/.zig-cache' \) -prune \
    -o \( -name '*.rs' -o -name '*.zig' -o -name '*.sh' -o -name 'Makefile' -o -name '*.qml' -o -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.hpp' \) \
    -type f -print | sort > "$tmp"

# sema-check:awk:start
#   purpose: per file, count opening and closing anchors by name and report
#            duplicates and mismatches
#   input:   file list in $tmp
#   output:  findings on stdout, awk exit status 1 if any
#   sideEffects: none
# sema-check:awk:end
awk '
function anchor_after(raw, tag,    m) {
    m = raw
    sub("^.*" tag, "", m)
    sub(/[^A-Za-z0-9_].*$/, "", m)
    return m
}
function reset(    n) {
    for (n in starts) delete starts[n]
    for (n in ends) delete ends[n]
}
function check_file(    n) {
    if (cur == "") return
    for (n in starts) {
        if (ends[n] != starts[n]) {
            printf("%s mismatched START_%s=%d END_%s=%d\n", cur, n, starts[n], n, ends[n] + 0)
            errors++
        }
    }
}
FILENAME != cur {
    check_file()
    reset()
    cur = FILENAME
}
/START_[A-Za-z0-9_]+/ {
    name = anchor_after($0, "START_")
    starts[name]++
    if (starts[name] == 2) {
        printf("%s:%d duplicate START_%s\n", FILENAME, FNR, name)
        errors++
    }
}
/END_[A-Za-z0-9_]+/ {
    name = anchor_after($0, "END_")
    ends[name]++
}
END {
    check_file()
    if (errors > 0) exit 1
}
' $(cat "$tmp") || fail "anchor validation failed"

echo "sema-check: OK"
