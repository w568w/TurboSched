# TurboSched
TurboSched: A Modern and Configurable Job Scheduling System

TurboSched aims to be a modern alternative to the traditional job schedulers, e.g. SLURM, PBS, etc. It is designed to be highly configurable and extensible, but mainly focuses on GPU clusters. 

**TurboSched is still under heavy development and is far from even a working prototype at the moment. Any contributions are welcome!**

## Build
1. Install Protobuf Compiler `protoc` and Go plugins ([guide](https://grpc.io/docs/languages/go/quickstart/)). Generate Go code from the proto files:
    ```bash
    protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative common/proto/*.proto
    ```
2. Copy `config.toml.example` to `config.toml` and modify the configuration file as needed.
3. Run on the nodes in the following order:
   ```bash
   go run turbod/main.go -c # Controller
   go run turbod/main.go -m # Compute Node
   ```
4. As a client, you can use the following command:
   ```bash
   go run turbo/main.go python # submit an interactive job running python
   go run turbo/main.go stop <job_id> # stop a job immediately
   ```

## Roadmap
**Note: Please do not rely on this roadmap as it is outdated. The issue page is the most up-to-date source of information.**
- [x] Basic single-node execution
- [x] Basic single-node scheduling
- [x] Task Cancellation
- [ ] GPU resource management
- [ ] GPU-aware scheduling
- [ ] Basic multi-node scheduling
- [ ] Failure-aware scheduling
- [ ] Multi-node discovery
- [ ] Task accounting
