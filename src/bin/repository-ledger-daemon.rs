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
    let configuration = nota_config::ConfigurationSource::from_argv()?.decode()?;
    repository_ledger::daemon::Daemon::new(configuration).run()
}
