#!/usr/bin/env bash

# Generate rust code from flatbuffers definitions in the arrow submodule.
#
# Adapted from https://github.com/apache/arrow-rs/blob/master/arrow-ipc/regen.sh

pushd "$(git rev-parse --show-toplevel)" || exit

flatc --filename-suffix "" --rust -o crates/rayexec_arrow_ipc/src/ submodules/arrow/format/*.fbs

pushd crates/rayexec_arrow_ipc/src/ || exit

# Common prefix content for all generated files.
PREFIX=$(cat <<'HEREDOC'
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(non_camel_case_types)]

use std::{cmp::Ordering, mem};
use flatbuffers::EndianScalar;

HEREDOC
)

SCHEMA_IMPORT="use crate::Schema::*;"
SPARSE_TENSOR_IMPORT="use crate::SparseTensor::*;"
TENSOR_IMPORT="use crate::Tensor::*;"

# For flatbuffer(1.12.0+), remove: use crate::${name}::\*;
names=("File" "Message" "Schema" "SparseTensor" "Tensor")

for file in *.rs; do
    if [ "$file" == "lib.rs" ]; then
        continue
    fi

    echo "Modifying file: $file"

    # Remove unnecessary module nesting, and duplicated imports.
    sed -i '/extern crate flatbuffers;/d' "$file"
    sed -i '/use self::flatbuffers::EndianScalar;/d' "$file"
    sed -i '/\#\[allow(unused_imports, dead_code)\]/d' "$file"
    sed -i '/pub mod org {/d' "$file"
    sed -i '/pub mod apache {/d' "$file"
    sed -i '/pub mod arrow {/d' "$file"
    sed -i '/pub mod flatbuf {/d' "$file"
    sed -i '/}  \/\/ pub mod flatbuf/d' "$file"
    sed -i '/}  \/\/ pub mod arrow/d' "$file"
    sed -i '/}  \/\/ pub mod apache/d' "$file"
    sed -i '/}  \/\/ pub mod org/d' "$file"
    sed -i '/use core::mem;/d' "$file"
    sed -i '/use core::cmp::Ordering;/d' "$file"
    sed -i '/use self::flatbuffers::{EndianScalar, Follow};/d' "$file"

    for name in "${names[@]}"; do
        sed -i "/use crate::${name}::\*;/d" "$file"
        sed -i "s/use self::flatbuffers::Verifiable;/use flatbuffers::Verifiable;/g" "$file"
    done

    if [ "$file" == "File.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "Message.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" "$SPARSE_TENSOR_IMPORT" "$TENSOR_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "SparseTensor.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" "$TENSOR_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "Tensor.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" | cat - "$file" > temp && mv temp "$file"
    else
        echo "$PREFIX" | cat - "$file" > temp && mv temp "$file"
    fi

done

popd || exit
cargo fmt -- crates/rayexec_arrow_ipc/src/*
