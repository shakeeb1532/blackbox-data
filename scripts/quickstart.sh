#!/usr/bin/env bash
set -euo pipefail

echo "Blackbox Data Pro quickstart"
echo "Creating virtual environment..."

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -e ".[pro]"

echo "Starting Blackbox Pro (wizard)..."
blackbox-pro wizard
