#!/bin/bash
# Control harness for go_check() per-case required semantics (eval-6 claim 5).
#
# Proves, WITHOUT running any real go test / PG, that the go_check()
# function in scripts/acceptance_matrix.sh enforces per-case required
# semantics:
#   C1  ordinary (req=0) all-PASS group         -> PASS, green
#   C2  required (req=1) all-SKIP group         -> SKIP / REQ_SKIP, red
#   C3  required (req=1) mixed PASS+SKIP group  -> SKIP / REQ_SKIP, red
#       (the masking defect: before the fix this recorded PASS=1 FAIL=0
#        REQ_SKIP=0 — an ordinary PASS hid the critical SKIP)
#   C4  required (req=1) all-PASS group         -> PASS, green
#   C5  required (req=1) parent PASS + INDENTED sub-case SKIP
#       -> SKIP / REQ_SKIP, red (go test -v indents sub-case result
#       lines by 4 spaces, so column-anchored regexes never saw them:
#       PASS=1 SKIP=0 masked the required sub-case SKIP)
#
# Technique (same as the external audit): the REAL record()/go_check()
# function bodies are extracted from acceptance_matrix.sh at runtime and
# eval'd, then go() is overridden to emit canned `go test -v` output.
# No go binary, no PG — pure control of the parser.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MATRIX="$SCRIPT_DIR/acceptance_matrix.sh"

# Fake go(): go_check runs `go test <pkg> -run <pat> ... -v 2>&1` and reads
# the output; emit canned -v output from FAKE_GO_OUT and exit 0.
go() { printf '%s\n' "$FAKE_GO_OUT"; }

# Pull the REAL functions (from `name() {` to the closing column-0 brace).
eval "$(sed -n '/^record() {/,/^}/p' "$MATRIX")"
eval "$(sed -n '/^go_check() {/,/^}/p' "$MATRIX")"

OK_PKG='github.com/agentium-lab/Janus/server/internal/fakepkg'
ALL_PASS_V="=== RUN   TestOrdinary
--- PASS: TestOrdinary (0.00s)
=== RUN   TestCritical
--- PASS: TestCritical (0.00s)
PASS
ok  	${OK_PKG}	0.012s"

ALL_SKIP_V="=== RUN   TestCritical
--- SKIP: TestCritical (0.00s)
PASS
ok  	${OK_PKG}	0.012s"

MIXED_V="=== RUN   TestOrdinary
--- PASS: TestOrdinary (0.00s)
=== RUN   TestCritical
--- SKIP: TestCritical (0.00s)
PASS
ok  	${OK_PKG}	0.012s"

# `go test -v` prints sub-case result lines INDENTED (4 spaces): a parent
# PASS + indented sub-SKIP must still record SKIP/REQ_SKIP for a required
# group — a required critical sub-case SKIP may never be masked by its
# passing parent.
INDENT_SUB_SKIP_V="=== RUN   TestGA02
=== RUN   TestGA02/required_pg_subcase
    --- SKIP: TestGA02/required_pg_subcase (0.00s)
--- PASS: TestGA02 (0.00s)
PASS
ok  	${OK_PKG}	0.012s"

control() { # id desc req canned_out
  local id="$1" desc="$2" req="$3" canned="$4"
  PASS=0; FAIL=0; SKIP=0; REQ_SKIP=0; RESULTS=(); REQ_SKIPPED_IDS=()
  FAKE_GO_OUT="$canned" go_check "$id" "$desc" ./fake/pkg 'TestFake' "$req"
  printf 'CONTROL %s [%s] -> PASS=%d FAIL=%d SKIP=%d REQ_SKIP=%d verdict=%s\n' \
    "$id" "$desc" "$PASS" "$FAIL" "$SKIP" "$REQ_SKIP" "${RESULTS[0]##*|*|}"
}

control C1 "ordinary-pass group (req=0)"                     0 "$ALL_PASS_V"
control C2 "all-critical-skip group (req=1)"                 1 "$ALL_SKIP_V"
control C3 "mixed ordinary-PASS + critical-SKIP (req=1)"     1 "$MIXED_V"
control C4 "all-pass required group (req=1)"                 1 "$ALL_PASS_V"
control C5 "parent-PASS masks indented sub-SKIP (req=1)"      1 "$INDENT_SUB_SKIP_V"
