# CMS Hospitals Sync

Downloads every dataset with theme "Hospitals" from the CMS Provider Data metastore,
converts CSV column names to snake_case, and only re-downloads datasets whose
`modified` date has changed since the previous run.

- Python 3.8+, standard library only; runs on Linux, macOS or Windows.
- Datasets are downloaded and processed in parallel (`--workers`, default 8).
- `state.json` in the output directory tracks last run time and per-dataset metadata.

```
python cms_hospitals_sync.py --out-dir cms_hospitals [--workers 8] [--force]
```

Scheduling examples (cron, Windows Task Scheduler) are in the script docstring.
