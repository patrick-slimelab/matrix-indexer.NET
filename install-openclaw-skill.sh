#!/bin/bash
# Install matrix-indexer skill to ~/.openclaw/skills/
# This makes the skill available to all OpenClaw agents on this machine.
#
# Usage: ./install-openclaw-skill.sh [--host hostname] [--user username]
#
# Examples:
#   ./install-openclaw-skill.sh              # local install
#   ./install-openclaw-skill.sh --host donghouse --user scoob  # remote install

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_NAME="matrix-indexer"
SKILL_DIR="$SCRIPT_DIR/skill/$SKILL_NAME"

# Parse args
HOST=""
USER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --host) HOST="$2"; shift 2 ;;
        --user) USER="$2"; shift 2 ;;
        *) shift ;;
    esac
done

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# Validate skill exists
if [ ! -f "$SKILL_DIR/SKILL.md" ]; then
    echo "ERROR: Skill not found at $SKILL_DIR/SKILL.md"
    exit 1
fi

# Install locally or remotely
if [ -z "$HOST" ]; then
    # Local install
    TARGET="$HOME/.openclaw/skills/$SKILL_NAME"
    log_info "Installing skill locally to $TARGET"
    mkdir -p "$TARGET"
    cp -r "$SKILL_DIR"/* "$TARGET/"
    log_success "Skill installed to $TARGET"
else
    # Remote install
    SSH_CMD="ssh ${USER:-$(whoami)}@$HOST"
    TARGET="\$HOME/.openclaw/skills/$SKILL_NAME"
    log_info "Installing skill remotely to $HOST"
    $SSH_CMD "mkdir -p $TARGET"
    scp -r "$SKILL_DIR"/* "${USER:-$(whoami)}@$HOST:$TARGET/"
    log_success "Skill installed to $HOST:$TARGET"
fi

echo ""
echo "Skill: $SKILL_NAME"
echo "Usage: matrix-indexer-search \"query\" [--limit N] [--room ROOM_ID]"
echo ""
echo "Restart OpenClaw or reload skills to activate."