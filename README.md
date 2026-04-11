# Matrix Indexer

A .NET console app that indexes Matrix messages to MongoDB for search and retrieval.

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

- MongoDB instance with `matrix_index` database
- `matrix-indexer-search` CLI tool installed on the target machine