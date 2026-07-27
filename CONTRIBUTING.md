## Run the server in dev mode:

```sh
cargo run --bin mokashop-server
```

## Query with grpcurl:

```sh
grpcurl -plaintext 127.0.0.1:50051 list
```

Describe a service:
```sh
grpcurl -plaintext 127.0.0.1:50051 describe mokashop.v1.BaristaService
```

Call an RPC:
```sh
grpcurl -plaintext -d '{"name": "Jerome"}' '127.0.0.1:50051' mokashop.v1.BaristaService/Hello
```

Note that's only possible because reflection is implemnted.
