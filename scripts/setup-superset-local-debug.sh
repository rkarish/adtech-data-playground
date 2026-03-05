#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Set up a local virtual environment for Superset script development
# =============================================================================
# Creates a Python virtual environment and installs dependencies so that
# linting and type checking work in the IDE (Pylance/Pyright).
#
# Prerequisites:
#   - Python 3.12+
#
# Usage:
#   bash scripts/setup-superset-local-debug.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"
SUPERSET_DIR="${PROJECT_ROOT}/superset"

# Create virtual environment if it doesn't exist
if [ ! -d "${VENV_DIR}" ]; then
  echo "==> Creating virtual environment at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

# Ensure pip is available (Debian/Ubuntu may omit it from venvs)
echo "==> Bootstrapping pip..."
"${VENV_DIR}/bin/python3" -m ensurepip --upgrade

# Upgrade pip
echo "==> Upgrading pip..."
"${VENV_DIR}/bin/python3" -m pip install --upgrade pip

# Install dependencies using the venv's python -m pip (more portable than bin/pip)
echo "==> Installing superset script dependencies..."
"${VENV_DIR}/bin/python3" -m pip install -r "${SUPERSET_DIR}/requirements.txt"

echo ""
echo "==> Local environment is ready."
echo "    Virtual environment: ${VENV_DIR}"
echo "    Select this interpreter in VS Code for import resolution."
