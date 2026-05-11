#!/usr/bin/env bash
# Set up a local Python venv and install dependencies.
# Run from the repo root:  bash setup.sh
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ ! -d ".venv" ]; then
  echo "Creating .venv with $($PYTHON_BIN --version)..."
  "$PYTHON_BIN" -m venv .venv
else
  echo ".venv already exists, reusing it."
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "Upgrading pip..."
python -m pip install --upgrade pip

echo "Installing requirements..."
python -m pip install -r requirements.txt

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp .env.example .env
  echo "Created .env from .env.example — fill in your Sussex credentials."
fi

echo
echo "Done. To start using it in this terminal:"
echo "    source .venv/bin/activate"