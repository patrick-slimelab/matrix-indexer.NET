#!/usr/bin/env bash
set -euo pipefail

# install.sh
#
# One-shot installer intended for: curl ... | sudo bash
#
# - Downloads latest GitHub Release asset (linux-x64)
# - Verifies sha256
# - Installs /usr/local/bin/matrix-indexer
# - Creates /etc/matrix-indexer/indexer.env (0600) without printing secrets
# - Optionally provisions MongoDB via Docker + systemd
# - Installs/enables matrix-indexer.service

REPO="${REPO:-patrick-slimelab/matrix-indexer.NET}"
VERSION="${VERSION:-latest}" # latest or vX.Y.Z
PREFIX="${PREFIX:-/usr/local/bin}"

# Optional override when OpenClaw/Clawdbot config is in a non-standard location:
#   OPENCLAW_CONFIG_PATH=/path/to/openclaw.json
OPENCLAW_CONFIG_PATH="${OPENCLAW_CONFIG_PATH:-}"

ASSET_TGZ="matrix-indexer-linux-x64.tar.gz"
ASSET_SHA="matrix-indexer-linux-x64.sha256"

SVC_USER="${SVC_USER:-matrix-indexer}"
SVC_GROUP="${SVC_GROUP:-matrix-indexer}"
STATE_DIR="${STATE_DIR:-/var/lib/matrix-indexer}"
LOG_DIR="${LOG_DIR:-/var/log/matrix-indexer}"
ENV_DIR="${ENV_DIR:-/etc/matrix-indexer}"
ENV_FILE="${ENV_FILE:-$ENV_DIR/indexer.env}"

MONGO_CONTAINER_NAME="${MONGO_CONTAINER_NAME:-matrix-indexer-mongo}"
MONGO_VOLUME_NAME="${MONGO_VOLUME_NAME:-matrix_indexer_mongo}"
MONGO_PORT="${MONGO_PORT:-27017}"
INSTALL_MONGO_DOCKER="${INSTALL_MONGO_DOCKER:-1}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: missing required command: $1" >&2
    exit 1
  }
}

need curl
need tar
need sha256sum

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: run as root (use: curl ... | sudo bash)" >&2
  exit 1
fi

# Resolve the invoking user's home dir (important because we're usually root via sudo)
INVOKER="${SUDO_USER:-}"
if [[ -z "$INVOKER" ]]; then
  INVOKER="$(logname 2>/dev/null || true)"
fi
if [[ -z "$INVOKER" ]]; then
  INVOKER="root"
fi

INVOKER_HOME="$(getent passwd "$INVOKER" | cut -d: -f6)"
if [[ -z "$INVOKER_HOME" ]]; then
  INVOKER_HOME="/root"
fi

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

resolve_latest_tag() {
  curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name" *: *"\([^"]*\)".*/\1/p' \
    | head -n1
}

TAG="$VERSION"
if [[ "$VERSION" == "latest" ]]; then
  echo "[install] Resolving latest release for $REPO"
  TAG="$(resolve_latest_tag)"
  if [[ -z "$TAG" ]]; then
    echo "ERROR: could not resolve latest release tag" >&2
    exit 1
  fi
fi

BASE_URL="https://github.com/${REPO}/releases/download/${TAG}"

echo "[install] Downloading ${ASSET_TGZ} (${TAG})"
curl -fsSL -o "$TMP/$ASSET_TGZ" "$BASE_URL/$ASSET_TGZ"
curl -fsSL -o "$TMP/$ASSET_SHA" "$BASE_URL/$ASSET_SHA"

cd "$TMP"

echo "[install] Extracting"
tar -xzf "$ASSET_TGZ"

echo "[install] Verifying checksums"
sha256sum -c "$ASSET_SHA"

install -d "$PREFIX"
install -m 0755 "$TMP/matrix-indexer" "$PREFIX/matrix-indexer"

# Install optional helper CLIs if the release asset contains them.
for helper in matrix-indexer-search matrix-indexer-delta; do
  if [[ -f "$TMP/$helper" ]]; then
    install -m 0755 "$TMP/$helper" "$PREFIX/$helper"
  fi
done

echo "[install] Installed binaries to $PREFIX"

install_jq_if_possible() {
  if command -v jq >/dev/null 2>&1; then return 0; fi
  if command -v apt-get >/dev/null 2>&1; then
    echo "[install] jq not found; attempting apt-get install jq" >&2
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y >/dev/null
    apt-get install -y jq >/dev/null
    command -v jq >/dev/null 2>&1 && return 0
  fi
  return 1
}

extract_with_jq() {
  local cfg="$1"
  local expr="$2"
  jq -r "$expr // empty" "$cfg" 2>/dev/null || true
}

extract_with_python() {
  local cfg="$1"
  local key="$2"
  local py=""
  if command -v python3 >/dev/null 2>&1; then py="python3"; fi
  if [[ -z "$py" ]] && command -v python >/dev/null 2>&1; then py="python"; fi
  [[ -n "$py" ]] || return 1
  "$py" - "$cfg" "$key" <<'PY' 2>/dev/null || true
import json, sys
cfg, key = sys.argv[1], sys.argv[2]
with open(cfg, 'r', encoding='utf-8') as f:
    data = json.load(f)
cur = data
for part in key.split('.'):
    if not isinstance(cur, dict):
        cur = None
        break
    cur = cur.get(part)
print(cur or '')
PY
}

collect_cfg_candidates() {
  local home="$1"
  echo "$home/.openclaw/openclaw.json"
  echo "$home/.moltbot/openclaw.json"
  echo "$home/.clawdbot/openclaw.json"
  echo "$home/.clawdbot/clawdbot.json"
}

CANDIDATES=()
if [[ -n "${OPENCLAW_CONFIG_PATH:-}" ]]; then
  CANDIDATES+=("$OPENCLAW_CONFIG_PATH")
fi

CANDIDATES+=( $(collect_cfg_candidates "$INVOKER_HOME") )

if [[ -d /home ]]; then
  while IFS= read -r d; do
    CANDIDATES+=("$d/.openclaw/openclaw.json")
    CANDIDATES+=("$d/.clawdbot/clawdbot.json")
  done < <(find /home -mindepth 1 -maxdepth 1 -type d 2>/dev/null || true)
fi

install_jq_if_possible || true

MATRIX_HOMESERVER="${MATRIX_HOMESERVER:-}"
MATRIX_USER_ID="${MATRIX_USER_ID:-}"
MATRIX_PASSWORD="${MATRIX_PASSWORD:-}"
MONGODB_URI_VALUE="${MONGODB_URI:-mongodb://127.0.0.1:${MONGO_PORT}}"
MONGODB_DB_VALUE="${MONGODB_DB:-matrix_index}"

# Best-effort OpenClaw/Clawdbot config detection. Config schemas vary, so try common keys.
if [[ -z "$MATRIX_HOMESERVER" || -z "$MATRIX_USER_ID" || -z "$MATRIX_PASSWORD" ]]; then
  for cfg in "${CANDIDATES[@]}"; do
    [[ -f "$cfg" ]] || continue
    if command -v jq >/dev/null 2>&1; then
      [[ -n "$MATRIX_HOMESERVER" ]] || MATRIX_HOMESERVER="$(extract_with_jq "$cfg" '.channels.matrix.homeserver // .matrix.homeserver // .plugins.entries.matrix.config.homeserver')"
      [[ -n "$MATRIX_USER_ID" ]] || MATRIX_USER_ID="$(extract_with_jq "$cfg" '.channels.matrix.userId // .channels.matrix.user_id // .matrix.userId // .matrix.user_id // .plugins.entries.matrix.config.userId // .plugins.entries.matrix.config.user_id')"
      [[ -n "$MATRIX_PASSWORD" ]] || MATRIX_PASSWORD="$(extract_with_jq "$cfg" '.channels.matrix.password // .matrix.password // .plugins.entries.matrix.config.password')"
    else
      [[ -n "$MATRIX_HOMESERVER" ]] || MATRIX_HOMESERVER="$(extract_with_python "$cfg" 'channels.matrix.homeserver' || true)"
      [[ -n "$MATRIX_USER_ID" ]] || MATRIX_USER_ID="$(extract_with_python "$cfg" 'channels.matrix.userId' || true)"
      [[ -n "$MATRIX_PASSWORD" ]] || MATRIX_PASSWORD="$(extract_with_python "$cfg" 'channels.matrix.password' || true)"
    fi

    if [[ -n "$MATRIX_HOMESERVER" || -n "$MATRIX_USER_ID" || -n "$MATRIX_PASSWORD" ]]; then
      echo "[install] Detected Matrix settings from: $cfg" >&2
      break
    fi
  done
fi

if [[ -z "$MATRIX_HOMESERVER" || -z "$MATRIX_USER_ID" || -z "$MATRIX_PASSWORD" ]]; then
  echo "[install] NOTE: could not fully auto-detect Matrix credentials from OpenClaw/Clawdbot config." >&2

  if [[ -t 0 ]]; then
    [[ -n "$MATRIX_HOMESERVER" ]] || read -r -p "MATRIX_HOMESERVER (e.g. https://matrix.example.org): " MATRIX_HOMESERVER || true
    [[ -n "$MATRIX_USER_ID" ]] || read -r -p "MATRIX_USER_ID (e.g. @bot:example.org): " MATRIX_USER_ID || true
    if [[ -z "$MATRIX_PASSWORD" ]]; then
      echo "[install] Paste Matrix password/access password (input hidden)." >&2
      read -r -s -p "MATRIX_PASSWORD: " MATRIX_PASSWORD || true
      echo >&2
    fi
  else
    echo "[install] Non-interactive shell: set MATRIX_HOMESERVER, MATRIX_USER_ID, and MATRIX_PASSWORD manually in $ENV_FILE" >&2
    echo "[install] Or rerun with env vars set before sudo/curl." >&2
  fi
fi

# Create service user/group and directories.
if ! getent group "$SVC_GROUP" >/dev/null; then
  groupadd --system "$SVC_GROUP"
fi

if ! id -u "$SVC_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SVC_GROUP" \
    --home-dir "$STATE_DIR" --create-home \
    --shell /usr/sbin/nologin \
    "$SVC_USER"
fi

install -d -o "$SVC_USER" -g "$SVC_GROUP" -m 0750 "$STATE_DIR"
install -d -o "$SVC_USER" -g "$SVC_GROUP" -m 0750 "$LOG_DIR"
install -d -o root -g root -m 0755 "$ENV_DIR"

touch "$LOG_DIR/matrix-indexer.log"
chown "$SVC_USER:$SVC_GROUP" "$LOG_DIR/matrix-indexer.log"
chmod 0644 "$LOG_DIR/matrix-indexer.log"

umask 077

if [[ ! -f "$ENV_FILE" ]]; then
  {
    echo "# matrix-indexer environment (generated by install.sh)"
    echo "MATRIX_HOMESERVER=\"${MATRIX_HOMESERVER}\""
    echo "MATRIX_USER_ID=\"${MATRIX_USER_ID}\""
    if [[ -n "$MATRIX_PASSWORD" ]]; then
      echo "MATRIX_PASSWORD=\"${MATRIX_PASSWORD}\""
    else
      echo "# MATRIX_PASSWORD not set. Add it here before running the indexer."
    fi
    echo "MONGODB_URI=\"${MONGODB_URI_VALUE}\""
    echo "MONGODB_DB=\"${MONGODB_DB_VALUE}\""
    echo "INDEXER_ID=\"${MATRIX_USER_ID:-default}\""
    echo "INDEXER_SYNC_TOKEN_PATH=\"${STATE_DIR}/sync_token.txt\""
    echo "INDEXER_BACKFILL_PAGE_SIZE=\"200\""
    echo "INDEXER_BACKFILL_WORKERS=\"2\""
    echo "INDEXER_JOINED_ROOMS_POLL_SECONDS=\"300\""
  } > "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "[install] Wrote $ENV_FILE (0600)"
else
  echo "[install] $ENV_FILE already exists; preserving existing values"
fi

if command -v systemctl >/dev/null 2>&1; then
  echo "[install] Installing systemd units"

  MANAGED_MONGO=0
  if [[ "$INSTALL_MONGO_DOCKER" == "1" && ( "$MONGODB_URI_VALUE" == mongodb://127.0.0.1:* || "$MONGODB_URI_VALUE" == mongodb://localhost:* ) ]]; then
    if command -v docker >/dev/null 2>&1; then
      MANAGED_MONGO=1
      cat > /etc/systemd/system/matrix-indexer-mongo.service <<MONGO_UNIT
[Unit]
Description=matrix-indexer MongoDB (docker container)
Requires=docker.service
After=docker.service

[Service]
Type=simple
ExecStartPre=-/usr/bin/docker volume create ${MONGO_VOLUME_NAME}
ExecStartPre=-/usr/bin/docker stop ${MONGO_CONTAINER_NAME}
ExecStartPre=-/usr/bin/docker rm ${MONGO_CONTAINER_NAME}
ExecStart=/usr/bin/docker run --rm --name ${MONGO_CONTAINER_NAME} -p 127.0.0.1:${MONGO_PORT}:27017 -v ${MONGO_VOLUME_NAME}:/data/db mongo:6
ExecStop=-/usr/bin/docker stop ${MONGO_CONTAINER_NAME}
Restart=always
RestartSec=3
TimeoutStartSec=60
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
MONGO_UNIT
    else
      cat >&2 <<'WARN'

====================  DOCKER NOT DETECTED  ====================
This installer can run MongoDB as a Docker container via systemd
(matrix-indexer-mongo.service), but Docker was not detected.

matrix-indexer will still be installed, but the default MONGODB_URI
points at localhost. Install Docker or provide another MongoDB before
expecting the service to stay healthy.
===============================================================

WARN
    fi
  fi

  SERVICE_REQUIRES=""
  SERVICE_AFTER="network-online.target"
  SERVICE_WANTS="network-online.target"
  if [[ "$MANAGED_MONGO" == "1" ]]; then
    SERVICE_REQUIRES="Requires=matrix-indexer-mongo.service"
    SERVICE_AFTER="network-online.target matrix-indexer-mongo.service"
  fi

  cat > /etc/systemd/system/matrix-indexer.service <<UNIT
[Unit]
Description=matrix-indexer (Matrix -> MongoDB)
After=${SERVICE_AFTER}
Wants=${SERVICE_WANTS}
${SERVICE_REQUIRES}

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_GROUP}
EnvironmentFile=${ENV_FILE}
WorkingDirectory=${STATE_DIR}
ExecStartPre=/bin/sh -lc 'mkdir -p ${LOG_DIR} && touch ${LOG_DIR}/matrix-indexer.log && chown ${SVC_USER}:${SVC_GROUP} ${LOG_DIR}/matrix-indexer.log && chmod 0644 ${LOG_DIR}/matrix-indexer.log'
ExecStartPre=/bin/bash -lc 'set -a; source ${ENV_FILE}; set +a; case "\${MONGODB_URI}" in mongodb://127.0.0.1:*|mongodb://localhost:*) ;; *) exit 0 ;; esac; port="\${MONGODB_URI##*:}"; port="\${port%%/*}"; for i in {1..90}; do (echo >/dev/tcp/127.0.0.1/\${port}) >/dev/null 2>&1 && exit 0; sleep 0.5; done; echo "Mongo not ready" >&2; exit 1'
ExecStart=${PREFIX}/matrix-indexer
Restart=always
RestartSec=2

StandardOutput=append:${LOG_DIR}/matrix-indexer.log
StandardError=append:${LOG_DIR}/matrix-indexer.log

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${STATE_DIR} ${LOG_DIR}

[Install]
WantedBy=multi-user.target
UNIT

  systemctl daemon-reload
  if [[ "$MANAGED_MONGO" == "1" ]]; then
    systemctl enable --now matrix-indexer-mongo.service || true
  fi
  systemctl enable --now matrix-indexer.service || systemctl restart matrix-indexer.service || true
  echo "[install] systemd: enabled+started matrix-indexer.service"
else
  echo "[install] NOTE: systemctl not found; skipping service installation" >&2
fi

cat <<EOF

[install] Done.
- Env file: $ENV_FILE
- Logs: ${LOG_DIR}/matrix-indexer.log
- Service: matrix-indexer.service (systemd)

Check status:
  systemctl status matrix-indexer.service --no-pager
  tail -n 200 ${LOG_DIR}/matrix-indexer.log

EOF
