#! /bin/bash
RELEASE="$1"

if [[ "$RELEASE" == "yes" ]]; then
echo "[INFO] building cli in release mode"
    RUSTFLAGS="-C target-cpu=native" cargo build --release
    cp target/release/cli bonerjams
else
echo "[INFO] building cli in debug mode"
    RUSTFLAGS="-C target-cpu=native" cargo build
    cp target/debug/cli bonerjams
fi
