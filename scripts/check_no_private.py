#!/usr/bin/env python3
"""Refuse to publish a tree that names a private deployment.

Not a secret scanner: no key, token or password value has ever been committed
here. This guards a different and easier-to-miss class -- a *current map of a
running deployment*. Host addresses, the port layout, the paths and modes of
the files that hold secrets, and the internal layout of the operator's hub are
worth more to an attacker than to a reader, and they leak by being written down
next to the code they describe.

Two things this exists because of, both real:
  * "which files are withheld" was mistaken for "where the private content is",
    so a scrub driven by a file-list diff missed the most sensitive file in the
    repository and left the same content alive in a staying file's history;
  * deleting a file does not remove its content from history.
So this checks CONTENT, and `--history` checks every commit, not just the tree.

Modes and their limits, stated because a gate trusted beyond its reach is worse
than none:
  * default -- two passes with different scopes, on purpose:
      - BOTH rule sets (the secrets rules: paths under a home, literal
        credentials, secret stores, records kept outside this repo, addresses of
        real machines; and the fleet-identity rules: the operator's task
        numbering, the names of the fleet's machines and roles, host-local
        deployment paths) over TRACKED files in the working tree.  An untracked
        file is invisible to it, which is correct for a pre-push gate (untracked
        files are not pushed) and wrong if you expect a general scanner.
        vendor/ is exempt, because nobody may edit it and a finding there is
        not actionable — see the note above FLEET_PATTERNS;
      - the identity rules AGAIN over the UNPUBLISHED RANGE: the commit messages
        of origin/main..HEAD and the lines those commits add.  This is the pass
        that would have caught the leak of task numbers and machine names into a
        public branch name and public commit messages, and it is the part no
        file scan can replace: a commit message is not a file.  It was once the
        ONLY scope of the identity rules, while main still carried the names it
        forbids — but a range-scoped gate cannot scrub a tree, because the tree
        to scrub is main, which a branch does not contain.  main has been
        scrubbed since, so the rules now run over the whole tree and the range
        pass is what catches a name that only ever appears in a message.
  * --history -- scans every commit reachable from HEAD, with BOTH rule sets.
    This is the audit view, and it reports occurrences main published before the
    scrub, which is correct: scrubbing a file does not scrub history.

Exit 0 clean, 1 on a finding, 2 when the run could not be completed (bad usage,
or the unpublished range could not be read — never reported as clean).
"""
import re, subprocess, sys

# Structural rules, deliberately not a list of literals we have already leaked.
# A gate whose patterns name the specific host, file and account it is guarding
# publishes a map of them the moment the gate itself is published -- and it can
# only ever catch the mistakes already made. These describe SHAPES instead, so a
# new host or a new credential trips them without anyone updating this file.
SAFE_IPV4 = re.compile(
    r'^(?:127\.|0\.0\.0\.0|255\.|224\.|239\.|169\.254\.'
    r'|192\.0\.2\.|198\.51\.100\.|203\.0\.113\.'      # RFC 5737 examples
    r'|10\.0\.0\.|192\.168\.1\.|172\.16\.0\.)'          # generic doc LANs
)
PATTERNS = [
    # any absolute path under a user's home on a specific machine
    (r'/root/(?!\.\.\.)[A-Za-z0-9_.-]+', 'host-local path under /root'),
    (r'/home/[A-Za-z0-9_.-]+/', 'host-local path under /home'),
    # an agent session scratchpad -- always machine- and session-specific
    (r'/tmp/(?:claude|opencode)-?\d*/[A-Za-z0-9_./-]+', 'agent session path'),
    # A credential assigned a QUOTED literal. The value must be quoted and must
    # not start with $ or a call: `let token = headers.get(..)` is a variable
    # binding, not a secret, and an earlier version of this rule flagged
    # eighteen of those. A checker that cries wolf gets switched off, so the
    # rule is narrow on purpose and OBVIOUS_DUMMY below carves out fixtures.
    (r'(?i)\b(?:pass(?:word)?|secret|token|api[_-]?key)\s*[:=]\s*'
     r'["\'][^"\'$<>{}\n]{6,}["\']', 'literal credential'),
    # An ABSOLUTE path into a directory that exists to hold credentials. Relative
    # or bare filenames are excluded: `node_ed25519.key` in the data directory is
    # a name in code, not a location on someone's disk.
    (r'/(?:[A-Za-z0-9_.-]+/)*(?:secrets?|artefacts?)/[A-Za-z0-9_.-]+',
     'absolute path into a secret store'),
    # A path under notes/ names a record kept outside this repository. Harmless
    # on its own and still worth refusing: it is how the naming of the private
    # records ends up published, one incidental mention at a time.
    (r'\bnotes/[A-Za-z0-9_.-]+\.md\b', 'reference to a record kept outside this repository'),
]

# Fixture values: present on purpose and not a disclosure.
OBVIOUS_DUMMY = re.compile(r'(?i)test|example|dummy|changeme|placeholder|xxxx|redacted')
# A literal credential inside a test is a fixture. This narrows the credential
# rule only -- every other rule still applies to test files, because a real host
# address in a test is exactly as published as one in main.rs.
TEST_FILE = re.compile(r'(?:^|/)tests?/|_test\.[a-z]+$|(?:^|/)test_')
COMPILED = [(re.compile(p), why) for p, why in PATTERNS]
IPV4 = re.compile(r'\b\d{1,3}(?:\.\d{1,3}){3}\b')

# ── Fleet identity ────────────────────────────────────────────────────────────
#
# The leak this family exists for: a branch name and a handful of commit
# messages in a PUBLIC repository carried the operator's own task numbers and
# the names of the machines in his fleet.  Nothing secret, and still a map.
#
# Three deliberate choices, because a gate trusted beyond its reach is worse
# than none:
#
#  * The tokens are assembled from FRAGMENTS.  Spelling out what the gate
#    forbids publishes the list the moment the gate itself is published — and
#    this script is inside the tree it guards, which is how an earlier tool in
#    this project reported itself as a finding.  The fragments are a second line
#    of defence; the self-exclusion below is the first.
#
#  * This family is scanned over the UNPUBLISHED RANGE — the commit messages of
#    origin/main..HEAD, and the lines those commits ADD — not over the whole
#    tree.  main still carries most of the names this family forbids, including
#    inside vendored third-party code that must not be edited, so a whole-tree
#    scan of this family would refuse every push from a clean branch, and a gate
#    that blocks all traffic gets switched off.  Clearing main's remaining
#    occurrences is its own job; until it lands, "clean" here means "nothing NEW
#    names the fleet", while --history still reports everything main published.
#
#  * It covers the shapes that were actually leaked, not every shape a future
#    machine could take: a NEW fleet node name is not caught until the token
#    list is extended.  A generic "word-digits" rule was rejected — it matches
#    sha-256, chacha20poly1305-0.10 and half the lockfile.
#
#  * vendor/ is exempt, and that is a decision about third-party code, not a
#    blind spot we chose to live with.  Vendored sources must not be edited, so
#    a finding there is unfixable by us: it can only be resolved by dropping the
#    dependency, and a gate that reports what nobody may repair is a gate that
#    gets switched off.  It is not hypothetical either — the pinned
#    zenoh-util-freebsd names a macOS package-manager directory that has
#    nothing to do with any deployment here, and the pinned obfs transport
#    carries its own HKDF domain-separation label, a protocol constant in
#    the same class as the mrgd-scope-key label the rule above refuses to
#    widen for.
_T_PLANCK = 'pla' + 'nck'
_T_BSDOS = 'bsd' + 'os'
_T_FEDORA = 'fed' + 'ora'
_T_AGENT = 'age' + 'nt'
_T_NODE = 'nod' + 'e'
_T_HOST = 'hos' + 't'
_T_MACHINE = 'mach' + 'ine'
_T_PEER = 'pe' + 'er'

FLEET_PATTERNS = [
    # The operator's task numbering, in both the bare and the prefixed form.
    (r'\b' + _T_PLANCK + r'-\d+\b', 'fleet task number'),
    (r'(?i)\b' + _T_PLANCK + r'\b', 'fleet node name'),
    (r'(?i)\btask-\d+\b', 'hub task number'),
    # Machine / role names of the fleet.
    (r'(?i)\b' + _T_BSDOS + r'-[a-z0-9]+(?:-\d+)?\b', 'fleet node name'),
    (r'(?i)\b' + _T_FEDORA + r'-\d+\b', 'fleet node name'),
    (r'(?i)\b' + _T_AGENT + r'-\d+\b', 'fleet agent name'),
    # The two roles that carry the operator's own name.  A generic "mrgd-.*"
    # rule was written first and rejected: it flags "mrgd-scope-key", the
    # domain-separation label in this project's own key derivation — a protocol
    # constant, not a machine.  Do not widen this back to the bare prefix.
    (r'\bmrgd-(?:' + 'bri' + 'dge|' + 'fi' + 'x)\b', 'fleet role name'),
    (r'\b[a-z]{3,12}-head\b', 'fleet role name'),
    # A bare machine number is only a machine number next to a machine word, or
    # as a pair.  A naked 185 is just as likely to be a port or a count.
    (r'(?i)\b(?:' + _T_NODE + r'|' + _T_AGENT + r'|' + _T_HOST + r'|' + _T_MACHINE
     + r'|' + _T_PEER + r')[- ]?18[56]\b', 'fleet node number'),
    (r'\b18[56]\s*/\s*18[56]\b', 'fleet node numbers'),
    # Where the deployment actually lives.
    (r'/opt/[A-Za-z0-9_.-]+', 'host-local deployment path'),
    (r'/srv/[A-Za-z0-9_.-]+', 'host-local deployment path'),
]
FLEET_COMPILED = [(re.compile(p), why) for p, why in FLEET_PATTERNS]
# This file is its own worst hit; never report it against itself.
SELF = 'check_no_private.py'
RANGE_BASE = 'origin/main'

def scan_tree(rev=None, fleet=False):
    """Findings in the tree at `rev` (or the working tree), both families if `fleet`."""
    if rev:
        files = subprocess.run(['git','ls-tree','-r','--name-only',rev],
                               capture_output=True, text=True).stdout.split('\n')
    else:
        files = subprocess.run(['git','ls-files'], capture_output=True, text=True).stdout.split('\n')
    rules = COMPILED + (FLEET_COMPILED if fleet else [])
    bad = []
    for f in files:
        # This file lists the patterns, so it matches every one of them. An
        # earlier tool in this project reported itself as a finding for exactly
        # this reason; a checker that cries wolf about itself gets switched off.
        # vendor/ is skipped for the same reason, one step further: nobody may
        # edit it, so a finding there is not actionable. See FLEET_PATTERNS.
        if (not f or f.startswith('target/') or f.startswith('vendor/')
                or f.endswith('check_no_private.py')):
            continue
        if rev:
            r = subprocess.run(['git','show',f'{rev}:{f}'], capture_output=True)
            if r.returncode: continue
            try: t = r.stdout.decode()
            except UnicodeDecodeError: continue
        else:
            try: t = open(f, encoding='utf-8').read()
            except (OSError, UnicodeDecodeError): continue
        is_test = bool(TEST_FILE.search(f))
        for rx, why in rules:
            if why == 'literal credential' and is_test:
                continue
            m = rx.search(t)
            if m and not OBVIOUS_DUMMY.search(m.group(0)):
                line = t[:m.start()].count('\n') + 1
                bad.append((f, line, m.group(0), why))
        # Any IPv4 that is not loopback, a documentation range or a generic
        # private example is an address of a real machine.
        for m in IPV4.finditer(t):
            if SAFE_IPV4.match(m.group(0)):
                continue
            line = t[:m.start()].count('\n') + 1
            bad.append((f, line, m.group(0), 'address of a specific machine'))
    return bad

def range_commits():
    """[(short hash, message)] for the commits not yet on the base ref.

    These are the commit messages that travel with the next push and are not
    already public.  Returns None when the base ref cannot be resolved, which
    the caller must treat as "could not check" rather than "clean".
    """
    r = subprocess.run(['git', 'log', '--format=%H%x1f%B%x1e', f'{RANGE_BASE}..HEAD'],
                       capture_output=True, text=True)
    if r.returncode:
        return None
    out = []
    for chunk in r.stdout.split('\x1e'):
        chunk = chunk.strip('\n')
        if not chunk:
            continue
        h, _, body = chunk.partition('\x1f')
        out.append((h.strip()[:9], body))
    return out

def range_added_lines():
    """[(file, line in the new file, text)] for the lines the range ADDS.

    The whole-tree scan answers "does the tree name the fleet", which is a
    question about what main already published.  This one answers "does THIS
    push", which is the question a pre-push gate can still act on.
    """
    r = subprocess.run(['git', 'diff', '-U0', f'{RANGE_BASE}...HEAD'],
                       capture_output=True, text=True)
    if r.returncode:
        return None
    added = []
    f, newno = None, 0
    for line in r.stdout.split('\n'):
        if not line:
            continue
        if line.startswith('+++ '):
            path = line[4:].strip()
            if path == '/dev/null':
                f = None
            else:
                f = path[2:] if path.startswith('b/') else path
            continue
        if line.startswith('---') or line.startswith('diff ') or line.startswith('index '):
            continue
        if line.startswith('@@'):
            # @@ -a,b +c,d @@ — the first number after '+' is the new start line.
            newno = int(line.split('+', 1)[1].split(' ', 1)[0].split(',')[0])
            continue
        if line.startswith('+'):
            added.append((f, newno, line[1:]))
            newno += 1
        elif not line.startswith('-'):
            newno += 1
    return added

def scan_fleet_range():
    """Fleet-identity findings in the unpublished range, or None if unscannable."""
    commits = range_commits()
    if commits is None:
        return None
    bad = []
    for h, msg in commits:
        for rx, why in FLEET_COMPILED:
            m = rx.search(msg)
            if m:
                bad.append((f'{h} (commit message)', 0, m.group(0), why))
    for f, line, text in (range_added_lines() or []):
        if not f or f.endswith(SELF):
            continue
        for rx, why in FLEET_COMPILED:
            m = rx.search(text)
            if m:
                bad.append((f, line, m.group(0), why))
    return bad

def main(argv):
    if len(argv) > 1 and argv[1] not in ('--history',):
        print(__doc__); return 2
    if len(argv) > 1:
        revs = subprocess.run(['git','rev-list','HEAD'], capture_output=True, text=True).stdout.split()
        total = 0
        for rev in revs:
            for f, line, hit, why in scan_tree(rev, fleet=True):
                print(f'{rev[:9]} {f}:{line}: {why}: {hit}'); total += 1
            # Commit messages are published with the tree. Three of them here
            # named a private path while every file in every tree was clean, so
            # a file-only scan reported a clean history that was not one.
            msg = subprocess.run(['git','log','-1','--format=%B',rev],
                                 capture_output=True, text=True).stdout
            for rx, why in COMPILED + FLEET_COMPILED:
                m = rx.search(msg)
                if m and not OBVIOUS_DUMMY.search(m.group(0)):
                    print(f'{rev[:9]} (commit message): {why}: {m.group(0)}'); total += 1
            for m in IPV4.finditer(msg):
                if SAFE_IPV4.match(m.group(0)):
                    continue
                print(f'{rev[:9]} (commit message): address of a specific machine: '
                      f'{m.group(0)}'); total += 1
        print(f'\ncheck_no_private: {len(revs)} commit(s) scanned, {total} finding(s)')
        return 1 if total else 0
    # Default mode: BOTH rule sets over the tracked files of the working tree
    # (the secrets rules are about content wherever it sits; the identity rules
    # are about content too, and a name in a file is published exactly as much
    # as a name in a commit message), PLUS the identity rules over the
    # UNPUBLISHED RANGE — the commit messages of origin/main..HEAD and the
    # lines those commits add, which is the part no file scan can reach.
    bad = scan_tree(fleet=True)
    for f, line, hit, why in bad:
        print(f'{f}:{line}: {why}: {hit}')
    fleet = scan_fleet_range()
    if fleet is None:
        # "Could not check" must never print as "clean".  The tree scan above
        # did run, so its findings are still on the way out; refuse to certify.
        print(f'\ncheck_no_private: {len(bad)} finding(s) in the working tree, but the '
              f'unpublished range ({RANGE_BASE}..HEAD) could not be read — not certifying. '
              f'Fetch {RANGE_BASE} (git fetch origin) and re-run.')
        return 2
    for f, line, hit, why in fleet:
        print(f'{f}:{line}: {why}: {hit}')
    commits = len(range_commits() or [])
    print(f'\ncheck_no_private: {len(bad)} finding(s) in the working tree, '
          f'{len(fleet)} finding(s) in {commits} unpublished commit(s) '
          f'({RANGE_BASE}..HEAD)')
    return 1 if (bad or fleet) else 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
