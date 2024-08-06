#!/usr/bin/env bash

curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip" -o protoc.zip
unzip -o protoc.zip
mv bin/protoc /bin/.
