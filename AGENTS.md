# repository-ledger — Agent Instructions

This repository is a triad runtime crate:

- `repository-ledger-daemon` is the long-lived owner of repository ledger state.
- `repository-ledger` is the thin ordinary Signal CLI client.
- `meta-repository-ledger` is the thin meta Signal CLI client.
- `signal-repository-ledger` is the ordinary peer contract.
- `meta-signal-repository-ledger` is the meta-signal authority contract.

Durable state must go through `sema-engine`; expose the store as
`repository-ledger.sema`, do not open a parallel redb handle or add ad-hoc JSON
state.
