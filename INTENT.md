# INTENT — repository-ledger

*What the psyche has explicitly intended for this project. Synthesised
from psyche statements and the applicable workspace constraints; not
embellished. Maintenance: `primary/skills/repo-intent.md`.*

`repository-ledger` is the triad runtime component that records pushed
repository changes from the local Gitolite server into a sema-engine
database. It is the `repository-ledger` CLI plus the long-lived
`repository-ledger-daemon`, with `meta-repository-ledger` as the
matching meta-signal CLI. Paired with the contract repos
`signal-repository-ledger` (ordinary receive-hook assertions and read
queries) and `meta-signal-repository-ledger` (meta-signal registration,
spool, and mirror policy).

## Repo-scope only

This file carries daemon-side intent for `repository-ledger`. Wire
vocabulary stays in `signal-repository-ledger/INTENT.md` and
`meta-signal-repository-ledger/INTENT.md`. Workspace-shape intent
stays in `primary/INTENT.md`.

## Goals

- Record repository events, commit observations, and file-change
  observations after they are pushed to the local Gitolite server,
  holding them in one `sema-engine` database as typed Rust records.
- Answer agent-facing discovery queries as first-class ordinary-contract
  `Query` operations: which repositories were edited recently, which
  files changed in a window, and commit-message search.

## Constraints

- **The CLIs talk only to their own daemon.** `repository-ledger` is the
  daemon's thin ordinary Signal client; `meta-repository-ledger` is the
  thin meta Signal client. The Gitolite receive hook invokes the
  ordinary CLI first, with a `ReceiveHookNotification` spool file as a
  fallback only when CLI submission fails.
- **Every stored record is a typed Rust record.** No line-oriented log
  is the source of truth.
- **Two authority tiers, two listener tasks.** Ordinary contract
  traffic and meta traffic have separate listener tasks; meta-signal
  configuration (registration, spool policy, mirror policy) arrives only
  through `meta-signal-repository-ledger`.
- **Store and spool are actor-owned concerns.** Daemon listeners ask a
  `RepositoryLedgerStoreActor`; fallback spool ingestion is triggered by
  a `SpoolIngestActor`. The old blocking listener loop, ad-hoc
  thread-spawned sockets, and repo-local frame IO module are retired.
- **One typed startup configuration.** The daemon starts from one typed
  signal-encoded rkyv `DaemonConfiguration` file on its single
  argument. Inline NOTA and `.nota` configuration files are authoring
  / CLI surfaces and are rejected by the daemon entrypoint.
- **Inter-component traffic is Signal; NOTA renders only at edges.**
  NOTA appears at the CLI / spool / debug edges; inter-component traffic
  is Signal frames.
- **Contract operations are domain verbs.** Discovery enters as `Query`
  and `Observe` operations carrying domain-noun payloads; the daemon
  lowers them internally to sema-engine work — the six Sema
  classification words never appear on the public wire.
- **Execution is Nexus-shaped, storage is SEMA-shaped.**
  Ordinary and meta Signal requests enter an internal typed
  `triad-runtime::Runner` path: Signal input becomes Nexus work, Nexus
  chooses SEMA read/write, SEMA applies or observes `sema-engine`, and
  Nexus replies to Signal. The old `signal-executor` lowering /
  command-executor path is retired. This is the execution-plane
  migration, not the separate contract schema migration.

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
- `../meta-signal-repository-ledger/INTENT.md` — meta-signal policy contract.
- `primary/skills/component-triad.md` — triad structure and wire layers.
- `primary/skills/contract-repo.md` — contract-local operation verbs.
