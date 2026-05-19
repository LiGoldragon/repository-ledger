fn main() {
    match repository_ledger::client::Client::run_from_environment() {
        Ok(reply) => println!("{reply}"),
        Err(error) => {
            eprintln!("(CliRejected \"{error}\")");
            std::process::exit(2);
        }
    }
}
