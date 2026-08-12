## Developer Documentation

### Build troubleshooting

> [!NOTE]
> If you see an error like `failed to authenticate when downloading repository: git@github.com:azat-rust/cursive`,
> it is likely because your local Git config is rewriting `https://github.com/` to `git@github.com:`:
>
> ```
> [url "git@github.com:"]
>     insteadOf = https://github.com/
> ```
>
> Cargo's built-in Git library does not handle this case gracefully.
> You can either remove that config entry or tell Cargo to use the system Git client instead:
>
> ```toml
> # ~/.cargo/config.toml
> [net]
> git-fetch-with-cli = true
> ```

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
