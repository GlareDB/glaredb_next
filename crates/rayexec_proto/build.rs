fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["proto/schema.proto"], &["proto"])?;

    Ok(())
}
