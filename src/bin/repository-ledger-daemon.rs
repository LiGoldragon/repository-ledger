fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    if arguments.len() != 2 || arguments[1].starts_with("--") {
        eprintln!(
            "(RepositoryLedgerDaemonRejected \"expected exactly one NOTA or Signal configuration argument\")"
        );
        std::process::exit(2);
    }

    println!("(RepositoryLedgerDaemonUnimplemented \"socket actors land in the next slice\")");
}
