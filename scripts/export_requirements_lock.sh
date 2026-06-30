#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PIP="$ROOT_DIR/backend/.venv/bin/pip"
OUT_FILE="$ROOT_DIR/backend/requirements.lock.txt"

if [[ ! -x "$VENV_PIP" ]]; then
  echo "Erro: pip da virtualenv não encontrado em $VENV_PIP" >&2
  echo "Crie/ative a venv em backend/.venv antes de gerar o lock." >&2
  exit 1
fi

TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

# Exclui o pacote editável local (-e /path) porque não é reproduzível dentro do container.
"$VENV_PIP" freeze | grep -vE '^-e ' > "$TMP_FILE" || true

if ! grep -qi '^argon2-cffi==' "$TMP_FILE"; then
  echo "argon2-cffi==25.1.0" >> "$TMP_FILE"
fi

mv "$TMP_FILE" "$OUT_FILE"
echo "Gerado: $OUT_FILE"
grep -E '^argon2-cffi==' "$OUT_FILE" || true

