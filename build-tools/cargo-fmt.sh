#!/bin/bash
set -e
if [ -d ./build-tools ]; then
    crates=(`find . -type f -name 'Cargo.toml'`)
    for crate in "${crates[@]}"; do
        echo "cargo +nightly fmt --manifest-path ${crate} --all"
        cargo +nightly fmt --manifest-path "${crate}" --all
    done
else
    echo "Please execute this script from the repository root."
fi
