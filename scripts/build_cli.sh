#! /bin/bash
RELEASE="$1"

if [[ "$RELEASE" == "yes" ]]; then
echo "[INFO] building cli in release mode"
    RUSTFLAGS="-C target-cpu=native" cargo build --release --bin cli
    cp target/release/cli bonerjams-cli
else
echo "[INFO] building cli in debug mode"
    RUSTFLAGS="-C target-cpu=native" cargo build --bin cli
    cp target/debug/cli bonerjams-cli
fi
