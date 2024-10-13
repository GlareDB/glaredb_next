#!/usr/bin/env bash

set -eux -o pipefail

GIT_ROOT=$(git rev-parse --show-toplevel)
pushd "$GIT_ROOT/crates/parquet_redux/"

# Generate the rust code (outputs to parquet.rs).
thrift -out ./src \
       --gen rs -r \
       "${GIT_ROOT}/submodules/parquet-format/src/main/thrift/parquet.thrift"

# Remove unused imports.
sed -i '/use thrift::server::TProcessor;/d' ./src/parquet.rs

# Replace `TSerializable`
sed -i 's/impl TSerializable for/impl crate::thrift_ext::TSerializable for/g' ./src/parquet.rs
sed -i 's/fn write_to_out_protocol(&self, o_prot: &mut dyn TOutputProtocol)/fn write_to_out_protocol<T: TOutputProtocol>(\&self, o_prot: \&mut T)/g' ./src/parquet.rs
sed -i 's/fn read_from_in_protocol(i_prot: &mut dyn TInputProtocol)/fn read_from_in_protocol<T: TInputProtocol>(i_prot: \&mut T)/g' ./src/parquet.rs

# Move it
mv ./src/parquet.rs ./src/thrift_gen.rs

popd
