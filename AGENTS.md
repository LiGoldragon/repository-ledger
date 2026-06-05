# repository-ledger — Agent Instructions

Read `~/primary/AGENTS.md`, then this file.

This repository is a triad runtime crate:

- `repository-ledger-daemon` is the long-lived owner of repository ledger state.
- `repository-ledger` is the thin CLI client.
- `signal-repository-ledger` is the ordinary peer contract.
- `meta-signal-repository-ledger` is the meta-signal authority contract.

Durable state must go through `sema-engine`; do not open a parallel redb handle
or add ad-hoc JSON state.
