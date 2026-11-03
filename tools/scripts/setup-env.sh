#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Define colors
BLUE='\033[1;34m'
GREEN='\033[1;32m'
NC='\033[0m' # No Color
INFO="${BLUE}ℹ${NC}"
OK="${GREEN}✓${NC}"

rm -f uv.toml

# Detect if running on internal Linux (Rodete)
if [ -f "/etc/os-release" ] && grep -q "rodete" /etc/os-release; then
    echo -e "${INFO} Detected internal environment (Rodete)."
    echo -e "${INFO} Creating uv.toml to configure public PyPI registry and mirror [tool.uv] settings..."
    cat <<EOF > uv.toml
exclude-newer = "7 days"

[exclude-newer-package]
advanced-alchemy = false
litestar = false
litestar-asyncapi = false
litestar-asyncpg = false
litestar-autowire = false
litestar-email = false
litestar-granian = false
litestar-htmx = false
litestar-mcp = false
litestar-oracledb = false
litestar-queues = false
litestar-saq = false
litestar-security = false
litestar-vite = false
polyfactory = false
pytest-databases = false
sqlglot = false
sqlglotc = false
sqlspec = false

[[index]]
name = "pypi"
url = "https://pypi.org/simple"
default = true
EOF
    echo -e "${OK} uv.toml configured."
else
    echo -e "${INFO} Not running on Rodete. Skipping internal environment setup."
fi
