fn main() {
    match repository_ledger::client::Client::run_meta_from_environment() {
        Ok(reply) => println!("{reply}"),
        Err(error) => {
            eprintln!("meta-repository-ledger: {error}");
            std::process::exit(2);
        }
    }
}
