"""Read mappings.xlsx and produce data/mappings.json for BL_DASH.html.

Usage:  python generate_mappings.py
Output: data/mappings.json

The JSON structure mirrors what the dashboard JS expects:
{
  "segments": { "<seg-id>": { "entity": "…", "ud3": "…" } },
  "scenarios": { "actual": [...], "le": [...], ... },
  "timePeriods": { "march": { "current": "2026M3", "py": "2025M3" }, ... },
  "mappings": {
    "<seg-id>": {
      "kpi_card":    [ { field, account, ud1, ud1Forecast, scale, type, computedFrom }, ... ],
      "kpi_tracker": [ ... ],
      "financial":   [ ... ],
      "yoy":         [ ... ],
      "outlook":     [ ... ],
      "headline":    [ ... ]
    }
  }
}
"""
import json
import os
import openpyxl

XLSX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mappings.xlsx")
OUT  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "mappings.json")


def read_sheet_rows(wb, sheet_name):
    """Return list of dicts from a worksheet (header row = keys)."""
    ws = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(h).strip() for h in rows[0]]
    return [dict(zip(headers, r)) for r in rows[1:] if any(c is not None for c in r)]


def main():
    if not os.path.exists(XLSX):
        print(f"ERROR: {XLSX} not found. Run create_template.py first.")
        return

    wb = openpyxl.load_workbook(XLSX, data_only=True, read_only=True)

    # ── Segments ────────────────────────────────────────────────
    seg_rows = read_sheet_rows(wb, "Segments")
    segments = {}
    for r in seg_rows:
        sid = str(r.get("segment_id", "")).strip()
        if sid:
            segments[sid] = {
                "brandName": str(r.get("brand_name", "")).strip(),
                "entity":    str(r.get("entity", "")).strip(),
                "ud3":       str(r.get("ud3", "")).strip(),
            }

    # ── Scenarios ───────────────────────────────────────────────
    sc_rows = read_sheet_rows(wb, "Scenarios")
    scenarios = {}
    for r in sc_rows:
        key = str(r.get("dashboard_scenario", "")).strip()
        vals = str(r.get("onestream_scenarios", "")).strip()
        if key and vals:
            scenarios[key] = [v.strip() for v in vals.split(";") if v.strip()]

    # ── Time Periods ────────────────────────────────────────────
    tp_rows = read_sheet_rows(wb, "Time Periods")
    timePeriods = {}
    for r in tp_rows:
        view = str(r.get("dashboard_view", "")).strip()
        if view:
            timePeriods[view] = {
                "current": str(r.get("current_time", "")).strip(),
                "py":      str(r.get("py_time", "")).strip(),
            }

    # ── Mappings ────────────────────────────────────────────────
    map_rows = read_sheet_rows(wb, "Mappings")
    mappings = {}

    for r in map_rows:
        seg  = str(r.get("segment", "") or "").strip()
        sec  = str(r.get("section", "") or "").strip()
        label = str(r.get("dashboard_label", "") or "").strip()
        if not seg or not sec or not label:
            continue

        entry = {
            "field": label,
        }

        acct = str(r.get("os_account", "") or "").strip()
        if acct:
            entry["account"] = acct

        ud1 = str(r.get("ud1", "") or "").strip()
        if ud1:
            entry["ud1"] = ud1

        ud1f = str(r.get("ud1_forecast", "") or "").strip()
        if ud1f:
            entry["ud1Forecast"] = ud1f

        scale = r.get("scale")
        if scale is not None:
            try:
                entry["scale"] = float(scale)
            except (ValueError, TypeError):
                entry["scale"] = 1

        vtype = str(r.get("value_type", "") or "").strip()
        if vtype:
            entry["type"] = vtype

        comp = str(r.get("computed_from", "") or "").strip()
        if comp:
            entry["computedFrom"] = comp

        notes = str(r.get("notes", "") or "").strip()
        if notes:
            entry["notes"] = notes

        mappings.setdefault(seg, {}).setdefault(sec, []).append(entry)

    wb.close()

    # ── Write JSON ──────────────────────────────────────────────
    output = {
        "segments":    segments,
        "scenarios":   scenarios,
        "timePeriods": timePeriods,
        "mappings":    mappings,
    }

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Stats
    total = sum(len(v) for sec in mappings.values() for v in sec.values())
    print(f"Written {OUT}")
    print(f"  {len(segments)} segments, {len(scenarios)} scenario groups, {len(timePeriods)} time periods")
    print(f"  {total} mapping entries across {len(mappings)} segment(s)")


if __name__ == "__main__":
    main()
