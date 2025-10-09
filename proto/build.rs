fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["src/ebi.proto"], &["src/"])?;
    Ok(())
}
