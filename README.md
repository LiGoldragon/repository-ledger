# repository-ledger

Triad runtime component for recording repository push events.

The current implementation provides:

- sema-engine storage for repository events, repository registrations, spool
  policy, and mirror policy records;
- `repository-ledger-daemon`, a long-lived daemon with separate ordinary and
  meta Signal sockets;
- `repository-ledger`, a thin ordinary-contract CLI client;
- `meta-repository-ledger`, a thin meta-contract CLI client;
- spool ingestion for the current CriomOS Gitolite receive-hook NOTA files.
