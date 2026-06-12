fn main() {
    match repository_ledger::client::Client::run_working_from_environment() {
        Ok(reply) => println!("{reply}"),
        Err(error) => {
            eprintln!("repository-ledger: {error}");
            std::process::exit(2);
        }
    }
}
