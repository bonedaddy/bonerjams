# bonerjams

Sled database wrapper to asist working with strogly typed data, along with raw binary data. Optional gRPC server and client is provided for integration within microservices.

# db

The `bonerjams-db` crate provides the typed client library for working with sled, while exposing the raw sled api when typing is too restrictive. the `rpc` module provides a key-value gRPC server with tls and token auth support, and a basic pubsub server.

The gRPC server accepts incoming key-values as raw bytes, however due to the nature of protocol buffers when using batch requests, keys must be base64 encoded.