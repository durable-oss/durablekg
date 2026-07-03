#!/usr/bin/env bash
#
# Build DurableKG binaries in release mode and install them to ~/.local/bin.
#
set -euo pipefail

# Resolve the workspace root (this script lives in <root>/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"

# Binaries to install: the `dkg` CLI (from apps/dkg-cli) and `dkg-server`.
BINARIES=(dkg dkg-server)

echo "==> Building release binaries in ${ROOT_DIR}"
cargo build --release \
    --manifest-path "${ROOT_DIR}/Cargo.toml" \
    --bin dkg \
    --bin dkg-server

echo "==> Installing to ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

for bin in "${BINARIES[@]}"; do
    src="${ROOT_DIR}/target/release/${bin}"
    if [[ ! -x "${src}" ]]; then
        echo "error: expected binary not found: ${src}" >&2
        exit 1
    fi
    install -m 0755 "${src}" "${INSTALL_DIR}/${bin}"
    echo "    ${bin} -> ${INSTALL_DIR}/${bin}"
done

echo "==> Done."

# Warn if the install dir isn't on PATH, so the binaries are actually runnable.
case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *) echo "note: ${INSTALL_DIR} is not on your PATH; add it to use the binaries." >&2 ;;
esac
