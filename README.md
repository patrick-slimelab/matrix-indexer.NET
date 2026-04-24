# Matrix Indexer

A .NET console app that indexes Matrix messages to MongoDB for search and retrieval.

## One-line install (Linux)

Installs the **latest GitHub Release** (`matrix-indexer` and `matrix-indexer-search`) to `/usr/local/bin`, writes `/etc/matrix-indexer/indexer.env`, and installs `matrix-indexer.service`. If Docker is available, the installer also manages MongoDB with `matrix-indexer-mongo.service`.

```bash
curl -fsSL https://raw.githubusercontent.com/patrick-slimelab/matrix-indexer.NET/master/install.sh | sudo bash
```

### Non-interactive install

```bash
sudo env MATRIX_HOMESERVER="https://matrix.example.org" \
  MATRIX_USER_ID="@bot:example.org" \
  MATRIX_PASSWORD="..." \
  bash install.sh
```

### OpenClaw/Clawdbot auto-config

If the installer detects an OpenClaw or Clawdbot config in a standard state dir, it will best-effort read Matrix settings and write `/etc/matrix-indexer/indexer.env` (mode `0600`) without printing secrets. You can also pass an explicit config path:

```bash
curl -fsSL https://raw.githubusercontent.com/patrick-slimelab/matrix-indexer.NET/master/install.sh | sudo env OPENCLAW_CONFIG_PATH=/path/to/openclaw.json bash
```

## Releases

Releases include:
- `matrix-indexer-linux-x64.tar.gz` (`matrix-indexer`, `matrix-indexer-search`, checksums)
- `matrix-indexer-linux-x64.sha256`

## OpenClaw Skill

This repo includes an OpenClaw skill for searching indexed Matrix messages.

### Install Skill (One-liner)

```bash
rm -rf ~/.openclaw/skills/matrix-indexer && mkdir -p ~/.openclaw/skills && git clone --depth 1 https://github.com/patrick-slimelab/matrix-indexer.NET /tmp/matrix-indexer && mv /tmp/matrix-indexer/skill/matrix-indexer ~/.openclaw/skills/ && rm -rf /tmp/matrix-indexer
```

### Alternative: Use extraDirs (no install needed)

Add to `~/.openclaw/openclaw.json`:

```json
{
  "skills": {
    "load": {
      "extraDirs": ["~/matrix-indexer.NET/skill"]
    }
  }
}
```

Then clone the repo:
```bash
git clone https://github.com/patrick-slimelab/matrix-indexer.NET ~/matrix-indexer.NET
```

### Usage

```bash
matrix-indexer-search "query" --limit 20 --room "!roomId:server"
```

## Requirements

- MongoDB instance with `matrix_index` database, or Docker for installer-managed MongoDB
- `matrix-indexer-search` CLI tool installed on the target machine for the OpenClaw search skill
