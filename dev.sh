#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"

info()  { echo "[dev] $*"; }
warn()  { echo "[dev] WARN: $*" >&2; }
die()   { echo "[dev] ERROR: $*" >&2; exit 1; }

RESET=0
for arg in "$@"; do
  [[ "$arg" == "--reset" ]] && RESET=1
done

PYTHON=$(command -v python3 2>/dev/null || command -v python)
[[ -z "$PYTHON" ]] && die "Python not found"

PY_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Using Python $PY_VERSION at $PYTHON"

if [[ $RESET -eq 1 && -d "$VENV" ]]; then
  info "Removing existing venv..."
  rm -rf "$VENV"
fi

if [[ ! -d "$VENV" ]]; then
  info "Creating virtual environment..."
  "$PYTHON" -m venv "$VENV"
fi

if [[ -f "$VENV/Scripts/activate" ]]; then
  source "$VENV/Scripts/activate"
elif [[ -f "$VENV/bin/activate" ]]; then
  source "$VENV/bin/activate"
else
  die "Could not find venv activate script"
fi

info "Venv active: $(which python)"
info "Installing/syncing dependencies..."
pip install -q -r "$ROOT/requirements.txt"

mkdir -p "$ROOT/data"

if [[ ! -f "$ROOT/.env" ]]; then
  if [[ -f "$ROOT/.env.example" ]]; then
    info "Copying .env.example -> .env"
    cp "$ROOT/.env.example" "$ROOT/.env"
    warn ".env created -- set GITHUB_TOKEN before the app can fetch data."
  fi
fi

if command -v powershell &>/dev/null; then
  stale_pids=$(powershell -Command "
    Get-NetTCPConnection -LocalPort 8055 -State Listen -ErrorAction SilentlyContinue |
      Select-Object -ExpandProperty OwningProcess -Unique
  " 2>/dev/null | tr -d '\r') || true
  if [[ -n "$stale_pids" ]]; then
    info "Killing stale processes on port 8055..."
    for pid in $stale_pids; do
      taskkill //F //PID "$pid" &>/dev/null && info "  killed PID $pid"
    done
    sleep 1
  fi
fi

info "Starting on http://localhost:8055 (hot-reload enabled)"
echo ""

cd "$ROOT"
exec uvicorn app.main:app \
  --host 127.0.0.1 \
  --port 8055 \
  --reload \
  --reload-dir app \
  --reload-dir static \
  --log-level info
