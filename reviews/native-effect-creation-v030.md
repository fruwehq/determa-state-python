# Native effect creation: bounded acceptance plan

Status: first independent review requires shared creation/journal tests before
implementation. Python implementation has not started.

This owner-local optional-host slice adds atomic creation to
`SQLiteCommittedEffectHost`. It does not change the portable core, add event
admission, implement a public transport endpoint, or start example integration.
Version 0.3.0 remains unreleased; schemas stay version 1 and machines `format: 1`.

## Fixed inputs and prior evidence

- Specification: `77c0a2e60cd0771a6d44ae170a079ddd51d7d9f0`, SPEC
  §§17.3, 18 and 19.1–19.2, 19.5.
- Conformance: `090aa297389e416121c25ef29fb0a75671d83233`, reviewed draft
  #114. The committed-native-effects generator exercises an existing admitted
  event producing an intent; its 62 vectors do not prove native creation.
- Python baseline: `39def1ac8268de55237f90e5cb8d7110fe27fc78`. The
  reviewed ownership boundary prevents ordinary adapters from bypassing native
  ownership. `seed` restores existing artifacts; it is not atomic core creation.
- Rust baseline: `cc24d28d4f88790e1bdc046dad01867110dfc2be`.
  `tests/native_handler.rs` already exercises actual creation, fresh trusted
  source selection, retained replay, changed identity, and SIGKILL fate. Credit
  that reviewed evidence; do not rewrite the existing implementation merely to
  match Python's internal layout.

## Contract and executable coverage

| Obligation | Existing contract/tests | Required Python evidence |
|---|---|---|
| Actual core initialization produces revision 0, creation receipt and every initial external intent | §17.3; portable checkpoint creation cases | Real format-1 initialization with multiple effects; no hand-assembled candidate checkpoint |
| Checkpoint, all pinned effect records, exact original request/first response and native authority role evidence commit together | §§18, 19.1–19.2 | One SQLite transaction; reopened exact bytes and matching root/history inventory |
| Equal creation replay survives later checkpoint changes and unavailable executable source without a core/provider call | §§17.3, 19.2 | Retain original response; check request equality before resolver activation, route lookup or CAS; current authority authorization still applies |
| Changed creation ID, definition or typed bindings cannot reuse an existing root | §17.3 | Exact conflict, unchanged checkpoint/journal/authority bytes and zero core/provider calls |
| Trusted configured executable source and exact pinned route are checked before core and again at commit | §19.2; existing route-change vectors | Refuse untrusted/mismatched source, invalid mapping or changed route generation; post-staging source/route loss rolls back all native rows |
| Every initial intent has its own exact effect ID/digest, token, handler/destination/mapping and runtime incarnation | §§19.1–19.2 | Compare actual initial emissions to all journal records; support empty initial outbox; reject unsupported token/target combinations before commit |
| No hidden bypass through a competing helper-only or ordinary checkpoint row | §§18, 19.2; completed ownership tests | Concurrent root creation serializes; pre-existing ownership is rejected or replayed through retained native evidence, never overwritten |
| Process death preserves one atomic fate | §19.5 | Real child SIGKILL after SQL staging before commit, and after commit before response; reopen, compare bytes, retry exactly once without dispatch |

The existing shared artifacts specify portable creation and journal semantics
separately; they do not execute their coupling at initialization. Add a shared
executable native-creation case before Python implementation: input is the actual
trusted definition, machine/root/creation identities, typed bindings and configured
route; observed output contains the exact actual creation checkpoint, receipt,
all initial external intents and journal pins, and retained first response. The
runner forwards inputs only. Independent semantic/hash checks must validate the
coupling, rather than accepting a copied expected result.

The additional acceptance cases must distinguish committed faulted initialization
at revision 0 from rejection before any aggregate exists. A rejection reserves no
root and permits a later valid creation. Preserve creation receipt sequence 0,
initialization lifecycle receipts in defined order, and internal mailbox emissions,
in addition to all initial external effects. Creation identity uses §17.3 normalized
typed-binding digest semantics, including numeric types and defaults, rather than
ad hoc raw request equality. Equal replay and conflict also apply after tombstoning.

Machine-visible operation tokens must originate in declared bindings/variables and
emitted data. A host-only token may instead use the §19.1 derivation, without
claiming that the machine already knows it. Do not interchange a separate optional
owner-local operation identity with the creation identity or imply §25 transport
semantics. The exact owner-local response contract must be fixed by these cases.

Fresh creation requires active authorized native authority and permanent allocation
evidence. It must not activate seed/imported artifacts, repair history gaps, or
overwrite pre-existing checkpoint/helper ownership. Recheck source and authorization
at the actual commit boundary. Creation makes zero SDK handler calls; independently
count zero core/provider calls on replay and conflict.

Language-native transaction, source-race and SIGKILL tests remain additional
required implementation evidence. The existing approved prose suffices; no new
portable behavior, grammar or schema version is proposed.

## Ordered checkpoints and reviews

1. Independently review this contract/test mapping before source changes.
2. Add and independently review the shared executable creation/journal cases,
   deterministic generator, semantic validator and input-only runner. Push this
   conformance checkpoint before Python source changes. Then add meaningful
   failing tests against the unchanged Python baseline, using the
   actual configured authority and handler fixtures. Record RED separately from
   setup failures. Commit and ordinary-push the explicitly unfinished checkpoint;
   verify the remote SHA.
3. Implement creation only. Freeze each meaningful checkpoint for focused checks
   and independent review; commit and ordinary-push corrections. Keep PR #111 draft.
4. At the final clean Python SHA run static checks, full unit/conformance tests,
   actual configured 62-vector effects/authority gate, actual PostgreSQL checks,
   and independent review. Parent gates are not current-head evidence.
5. Review Rust parity against that acceptance set. Reuse exact-head existing
   evidence where unchanged; run focused additional tests for any new case, and
   review/gate any correction separately. No parallel Cargo runs.
6. Mark creation complete in the cross-repository audit only after these proofs.
   Choose event admission as its own next slice. Examples and publication remain
   blocked on their wider dependencies.

No force pushes, main writes, tags, package publication, release dispatch or
deployment are part of these checkpoints.
