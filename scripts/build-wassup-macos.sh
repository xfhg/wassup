#!/bin/bash
# Build and ad-hoc sign the private macOS app. Does not install or launch it.
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo"
build_target="${WASSUP_TARGET:-aarch64-apple-darwin}"
build_directory="${CARGO_TARGET_DIR:-$repo/target}"
# Keep Cargo and the bundler on the same directory even if Cargo config sets one.
export CARGO_TARGET_DIR="$build_directory"
build_version="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["version"])')"

cargo build --locked --release --bin wassup --target "$build_target"
mkdir -p "$repo/dist"
"$repo/packaging/macos/bundle.sh" \
    "$build_directory/$build_target/release/wassup" \
    "$repo/dist/Wassup.app" "$build_version"
