{
  description = "repository-ledger — Gitolite repository event ledger daemon";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-analyzer"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        src = craneLib.cleanCargoSource ./.;
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      in
      {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        });

        checks = {
          build = craneLib.cargoBuild (commonArgs // {
            inherit cargoArtifacts;
          });

          test = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
          });

          fmt = craneLib.cargoFmt {
            inherit src;
          };
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/repository-ledger";
        };

        devShells.default = pkgs.mkShell {
          name = "repository-ledger";
          packages = [
            pkgs.jujutsu
            toolchain
          ];
        };
      }
    );
}
