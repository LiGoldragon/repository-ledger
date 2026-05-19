fn main() {
    match repository_ledger::client::RepositoryLedgerClient::run_from_environment() {
        Ok(reply) => println!("{reply}"),
        Err(error) => {
            eprintln!("(RepositoryLedgerCliRejected \"{error}\")");
            std::process::exit(2);
        }
    }
}
