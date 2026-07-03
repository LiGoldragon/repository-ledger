use std::{env, path::PathBuf};

use schema_rust::{
    MetaListenerTier, NexusDaemonShape, SocketModeBits, WorkingListenerTier,
    build::{GenerationDriver, GenerationPlan, ModuleEmission},
};

const META_SOCKET_MODE: u32 = 0o600;

fn main() {
    SchemaBuild::from_environment().run();
}

struct SchemaBuild {
    crate_root: PathBuf,
}

impl SchemaBuild {
    fn from_environment() -> Self {
        Self {
            crate_root: PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir set")),
        }
    }

    fn run(&self) {
        println!("cargo:rerun-if-changed=src/schema/daemon.rs");

        let plan = GenerationPlan::new(&self.crate_root, "repository_ledger", "0.1.0")
            .with_module(ModuleEmission::daemon_module("nexus", Self::daemon_shape()));
        GenerationDriver::new(plan)
            .generate()
            .expect("generate repository-ledger schema artifacts")
            .write_or_check("REPOSITORY_LEDGER_UPDATE_SCHEMA_ARTIFACTS")
            .expect("checked-in repository-ledger schema artifacts are fresh");
    }

    fn daemon_shape() -> NexusDaemonShape {
        NexusDaemonShape::new(
            "repository-ledger-daemon",
            WorkingListenerTier::component_decoded(),
        )
        .with_meta_tier(MetaListenerTier::new(SocketModeBits::new(META_SOCKET_MODE)))
    }
}
