#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
proto_dir="${repo_root}/proto"
out_dir="${repo_root}/turntf/_generated"

if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc is required but was not found in PATH" >&2
  exit 1
fi

mkdir -p "${out_dir}"

(
  cd "${repo_root}"
  protoc \
    --proto_path="${proto_dir}" \
    --python_out="${out_dir}" \
    "${proto_dir}/client.proto"
)

echo "generated proto into ${out_dir}"
