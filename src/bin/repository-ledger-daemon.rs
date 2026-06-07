fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("(DaemonRejected \"{error}\")");
            std::process::exit(2);
        }
    }
}

fn run() -> repository_ledger::Result<()> {
    repository_ledger::RepositoryLedgerDaemonCommand::from_environment().run()
}
