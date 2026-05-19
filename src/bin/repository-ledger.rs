fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    if arguments.len() != 2 || arguments[1].starts_with("--") {
        eprintln!("(RepositoryLedgerCliRejected \"expected exactly one NOTA or Signal argument\")");
        std::process::exit(2);
    }

    println!("(RepositoryLedgerCliUnimplemented \"daemon Signal client lands in the next slice\")");
}
