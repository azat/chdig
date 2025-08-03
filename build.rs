fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated") // Or anywhere you prefer
        .compile(&["proto/perfetto_trace.proto"], &["proto"])
        .unwrap();
}