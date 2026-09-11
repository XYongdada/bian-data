# Binance Market Data

Repository for public market datasets used by the `lianghua` research project.

No datasets are currently published.

## Publishing BTCUSDT aggTrades

`scripts/publish_aggtrades_release.py` streams the local Binance ZIP archives,
creates time-named `.tar.xz` shards smaller than 95 MiB, uploads each shard to
yearly GitHub Releases, verifies the remote size, and deletes successful local
temporary files. Progress is saved under `.publish-state/`, so rerunning the
same command resumes from the last completed chunk.

```powershell
python scripts/publish_aggtrades_release.py
```

If direct GitHub API access is unstable, pass the local proxy explicitly:

```powershell
python scripts/publish_aggtrades_release.py --proxy http://127.0.0.1:7890
```

Use `--max-shards 1 --compress-only --keep-local` for a one-shard local test.
Authenticate once before uploading with `gh auth login`; a portable GitHub CLI
may also be placed at `.tools/bin/gh.exe`.
