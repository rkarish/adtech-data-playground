#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Set up a local debug environment for the mock data generator
# =============================================================================
# Creates a Python virtual environment, installs dependencies, and ensures
# Kafka is running. Use the VS Code launch configuration to run/debug the
# generator after this script completes.
#
# Prerequisites:
#   - Python 3.12+
#   - Kafka running (e.g. via `docker compose up kafka -d`)
#
# Usage:
#   bash scripts/setup-generator-local-debug.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"
MOCK_DIR="${PROJECT_ROOT}/mock-data-gen"

# Start Kafka if not already running
echo "==> Ensuring Kafka is running..."
docker compose -f "${PROJECT_ROOT}/docker-compose.yml" up kafka -d

echo "==> Waiting for Kafka to be healthy..."
until docker compose -f "${PROJECT_ROOT}/docker-compose.yml" ps kafka --format json | grep -q '"Health":"healthy"'; do
  sleep 2
done
echo "==> Kafka is ready."

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
echo "==> Installing mock-data-gen package..."
"${VENV_DIR}/bin/python3" -m pip install "${MOCK_DIR}"

echo ""
echo "==> Local debug environment is ready."
echo "    Virtual environment: ${VENV_DIR}"
echo "    Use the VS Code launch configuration to run/debug the generator."
