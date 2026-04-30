#!/usr/bin/env bash
# Wipe local service outputs without touching the database.
# Useful between manual test runs.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf state lake share events
echo "removed: state/ lake/ share/ events/"
