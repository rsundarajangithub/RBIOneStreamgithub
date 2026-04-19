# `data/` -- generated dashboard payloads

This folder is **populated by the GitHub Action** in
`.github/workflows/fetch-onestream-data.yml`.

Files committed here:

| File             | Source                                   | Notes                              |
| ---------------- | ---------------------------------------- | ---------------------------------- |
| `dashboard.json` | `transform.py` (after fetching brucetest) | Consumed by `RBI-Close-Dashboard.html` on load. |
| `meta.json`      | `transform.py`                            | Footer metadata (timestamp, period). |

Files **not** committed:

| File                    | Why                                                            |
| ----------------------- | -------------------------------------------------------------- |
| `brucetest_export.json` | Raw cube extract, ~1 MB; regenerated every action run.         |

To refresh manually, trigger the action via GitHub UI -> Actions -> "Fetch OneStream Data" -> Run workflow.
