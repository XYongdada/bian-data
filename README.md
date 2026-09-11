# Binance Market Data

Historical public market datasets used by the `lianghua` research project.

## Included datasets

### BTCUSDT USD-M Futures metrics

- Source: Binance public data archive
- Market: USD-M Futures
- Symbol: `BTCUSDT`
- Cadence: daily archive
- Coverage: `2020-09-01` through `2026-09-07`
- Files: 2,198 ZIP archives
- Path: `data/futures/um/daily/metrics/BTCUSDT/`

Each ZIP is kept in the original Binance archive format and filename. More
datasets may be added separately in the future.

## Browser-readable CSV

The same metrics are also available as uncompressed, yearly CSV files under
`csv/futures/um/daily/metrics/BTCUSDT/`. These files can be opened directly on
GitHub without downloading or extracting the source archives.

Run `python scripts/convert_metrics_to_csv.py` to rebuild them from the ZIP
archives. Rows are deduplicated by `create_time` and retain Binance's original
column names and values.
