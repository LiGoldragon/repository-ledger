use repository_ledger::{DaemonEntry, RepositoryLedgerProcessDaemon};

fn main() -> std::process::ExitCode {
    <RepositoryLedgerProcessDaemon as DaemonEntry>::run_to_exit_code()
}
