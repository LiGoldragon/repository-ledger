# INTENT — repository-ledger

*What the psyche has explicitly intended for this project. Synthesised
from psyche statements and the applicable workspace constraints; not
embellished. Maintenance: `primary/skills/repo-intent.md`.*

`repository-ledger` is the triad runtime component that records pushed
repository changes from the local Gitolite server into a sema-engine
database. It is the `repository-ledger` CLI plus the long-lived
`repository-ledger-daemon`. Paired with the contract repos
`signal-repository-ledger` (ordinary receive-hook assertions and read
queries) and `owner-signal-repository-ledger` (owner-only registration,
spool, and mirror policy).

## Repo-scope only

This file carries daemon-side intent for `repository-ledger`. Wire
vocabulary stays in `signal-repository-ledger/INTENT.md` and
`owner-signal-repository-ledger/INTENT.md`. Workspace-shape intent
stays in `primary/INTENT.md`.

## Goals

- Record repository events, commit observations, and file-change
  observations after they are pushed to the local Gitolite server,
  holding them in one `sema-engine` database as typed Rust records.
- Answer agent-facing discovery queries as first-class ordinary-contract
  `Query` operations: which repositories were edited recently, which
  files changed in a window, and commit-message search.

## Constraints

- **The CLI talks only to its own daemon.** The `repository-ledger` CLI
  is the daemon's thin Signal client; the Gitolite receive hook invokes
  it first, with a `ReceiveHookNotification` spool file as a fallback
  only when CLI submission fails.
- **Every stored record is a typed Rust record.** No line-oriented log
  is the source of truth.
- **Two authority tiers, two listener actors.** Ordinary contract
  traffic and owner traffic have separate listener actors; owner-only
  configuration (registration, spool policy, mirror policy) arrives only
  through `owner-signal-repository-ledger`.
- **One typed startup configuration.** The daemon starts from one typed
  `DaemonConfiguration` record on its single argument.
- **Inter-component traffic is Signal; NOTA renders only at edges.**
  NOTA appears at the CLI / spool / debug edges; inter-component traffic
  is Signal frames.
- **Contract operations are domain verbs.** Discovery enters as `Query`
  and `Observe` operations carrying domain-noun payloads; the daemon
  lowers them internally to sema-engine work — the six Sema
  classification words never appear on the public wire.

## Anti-patterns

- This component does not own the Gitolite installation (CriomOS owns
  the service), GitHub mirroring execution in the first slice, or report
  authoring / commit-message policy.
- Time-window comparison over `Timestamp(String)` in canonical
  UTC-sortable form is acceptable only transitionally; it should collapse
  into native timestamp comparison when the workspace timestamp type
  lands.

## See also

- `ARCHITECTURE.md` — component shape, owned/not-owned boundaries,
  query shapes, current slice.
- `../signal-repository-ledger/INTENT.md` — ordinary hook + query contract.
- `../owner-signal-repository-ledger/INTENT.md` — owner-only policy contract.
- `primary/skills/component-triad.md` — triad structure and wire layers.
- `primary/skills/contract-repo.md` — contract-local operation verbs.
