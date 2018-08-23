extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("proto/wal.capnp")
        .run()
        .expect("Compile command failed?")
}
