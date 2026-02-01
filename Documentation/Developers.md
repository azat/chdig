## Developer Documentation

### Debugging async code with tokio-console

chdig supports [tokio-console](https://github.com/tokio-rs/console) for debugging async tasks and runtime behavior.

To enable tokio console support:

1. Build with the `tokio-console` feature:
   ```bash
   cargo build --features tokio-console
   ```

2. Run chdig:
   ```bash
   cargo run --features tokio-console
   ```

3. In a separate terminal, start tokio-console:
   ```bash
   # Install if needed
   cargo install tokio-console

   # Connect to the running application
   tokio-console
   ```
