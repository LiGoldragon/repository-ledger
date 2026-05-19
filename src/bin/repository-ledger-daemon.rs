fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("(RepositoryLedgerDaemonRejected \"{error}\")");
            std::process::exit(2);
        }
    }
}

fn run() -> repository_ledger::Result<()> {
    let configuration = nota_config::ConfigurationSource::from_argv()?.decode()?;
    repository_ledger::daemon::RepositoryLedgerDaemon::new(configuration).run()
}
