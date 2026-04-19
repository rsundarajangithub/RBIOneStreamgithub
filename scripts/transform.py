#!/usr/bin/env python3
"""Transform raw brucetest cube cells into the nested DASHBOARD_DATA shape.

Inputs (relative to repo root):
    data/brucetest_export.json         OneStream-style {"Tables":[{"Columns":[...],"Rows":[[...]]}]}
                                         -- produced by the GitHub Action that
                                         queries brucetest via the SqlAdapter.
    config/dashboard_mapping.csv       295-row slot -> cube-intersection map
                                         (built once by build_dashboard_mapping.ps1
                                         and committed to the repo).
    config/period.json                  Optional. Defines which OneStream
                                         (scenario, time) tuple feeds each
                                         dashboard column for each period.
                                         If absent, the March 2026 close
                                         defaults below are used.

Output:
    data/dashboard.json                Drop-in replacement for the
                                         window.DASHBOARD_DATA assignment in
                                         RBI-Close-Dashboard.html.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CONFIG_DIR = ROOT / "config"

BRUCETEST_PATH = DATA_DIR / "brucetest_export.json"
MAPPING_PATH = CONFIG_DIR / "dashboard_mapping.csv"
PERIOD_PATH = CONFIG_DIR / "period.json"
OUTPUT_PATH = DATA_DIR / "dashboard.json"
META_PATH = DATA_DIR / "meta.json"

# ---------- defaults: March 2026 close -----------------------------------
# Source: CONTROL sheet of Brand FPA Close Master_vMar26_vTest2.xlsx.
# Override at deploy time by writing config/period.json.
DEFAULT_PERIOD: dict[str, Any] = {
    "label": "March 2026",
    "close_year": 2026,
    "periods": {
        "month":   {"cy": "2026M3", "py": "2025M3"},
        "quarter": {"cy": "2026Q1", "py": "2025Q1"},
        "ytd":     {"cy": "2026Q1", "py": "2025Q1"},
    },
    # dashboard_column -> (os_scenario, period_side)
    # period_side picks "cy" or "py" from the period dict above.
    "scenarios": {
        "actual": {"os_scenario": "Actual",         "side": "cy"},
        "le":     {"os_scenario": "Forecast_02_10", "side": "cy"},
        "budget": {"os_scenario": "Budget",         "side": "cy"},
        "py":     {"os_scenario": "Actual",         "side": "py"},
    },
}

# Segment metadata (preserve naming + currency from embedded dashboard).
SEGMENT_META = [
    ("th-cus",    "TH C&US",    "TH",   "CAD"),
    ("bk-usc",    "BK US&C",    "BK",   "USD"),
    ("plk-usc",   "PLK US&C",   "PLK",  "USD"),
    ("fhs-usc",   "FHS US&C",   "FHS",  "USD"),
    ("rbi-intl",  "RBI INTL",   "INTL", "USD"),
    ("rbi-level", "RBI",        "RBI",  "USD"),
]


# ---------- helpers ------------------------------------------------------

def load_period_config() -> dict[str, Any]:
    if PERIOD_PATH.exists():
        with PERIOD_PATH.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        # shallow-merge with defaults so partial overrides work
        out = json.loads(json.dumps(DEFAULT_PERIOD))
        out.update(cfg)
        return out
    return DEFAULT_PERIOD


def load_brucetest() -> tuple[list[str], list[list[Any]]]:
    """Load OneStream SqlAdapter response -> (cols, rows)."""
    if not BRUCETEST_PATH.exists():
        sys.stderr.write(f"FATAL: {BRUCETEST_PATH} missing.\n")
        sys.exit(1)

    # utf-8-sig tolerates a BOM if some upstream tool wrote one.
    with BRUCETEST_PATH.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)

    tables = payload.get("Tables") or payload.get("tables") or []
    if not tables:
        sys.stderr.write("FATAL: brucetest_export.json has no Tables.\n")
        sys.exit(1)

    t = tables[0]
    cols = [c.get("ColumnName") or c.get("Name") for c in t.get("Columns", [])]
    rows = t.get("Rows", [])
    return cols, rows


def index_brucetest(cols: list[str], rows: list[list[Any]]) -> dict:
    """Build a lookup keyed by the same fields the mapping uses."""
    col_idx = {c.lower(): i for i, c in enumerate(cols) if c}

    def get(row: list[Any], name: str) -> str:
        i = col_idx.get(name.lower())
        if i is None:
            return ""
        v = row[i]
        return "" if v is None else str(v)

    def numeric(row: list[Any], name: str) -> float:
        i = col_idx.get(name.lower())
        if i is None:
            return 0.0
        v = row[i]
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    # Index: (entity, scenario, time, account, ud1, ud2, ud3, ud4, ud5, ud6, ud7, ud8) -> amount
    idx: dict[tuple, float] = {}
    for row in rows:
        key = (
            get(row, "entity"),
            get(row, "scenario"),
            get(row, "time_member"),
            get(row, "os_account"),
            get(row, "ud1"),
            get(row, "ud2"),
            get(row, "ud3"),
            get(row, "ud4"),
            get(row, "ud5"),
            get(row, "ud6"),
            get(row, "ud7"),
            get(row, "ud8"),
        )
        idx[key] = numeric(row, "amount")

    return idx


def lookup_amount(brucetest: dict, mrow: dict[str, str], scenario: str, time_member: str) -> float:
    key = (
        mrow["entity"],
        scenario,
        time_member,
        mrow["account"],
        mrow["ud1"], mrow["ud2"], mrow["ud3"], mrow["ud4"],
        mrow["ud5"], mrow["ud6"], mrow["ud7"], mrow["ud8"],
    )
    return brucetest.get(key, 0.0)


def safe_div(num: float, den: float) -> float:
    if not den:
        return 0.0
    return num / den


# ---------- calculated KPI metrics --------------------------------------
# These are derived in the transformer because the workbook treats them as
# computed fields, not stored OneStream accounts.  Hard-coded coordinates
# below are taken from how the SSS_SST OS / NRG OS sheets define them.

CALC_BASE: dict[str, dict[str, str]] = {
    # Account used for store-count based metrics (Unit %).
    "store_count": {"account": "SystemStoreCount"},
    # Sales used for SWSG weighting.
    "total_sales": {"account": "TotalSales"},
    # AOI for AOIG.
    "aoi":         {"account": "AOI"},
    # Compset Sales CY for SSS reuse in SWSG (already covered separately).
    "sales_cy":    {"account": "ReportedSystemCompSalesCY"},
}


def calc_unit_pct(brucetest: dict, segment: str, cy_time: str, py_time: str, scenario_cy: str, scenario_py: str) -> float:
    """Unit % = (StoreCount_CY / StoreCount_PY) - 1  for the segment rollup."""
    # Use Top entity at segment-level proxy (best we can do without specific rule).
    base = {
        "entity": "RestaurantBrandsIntlMNGT",
        "account": "SystemStoreCount",
        "ud1": "Top", "ud2": "Top",
        "ud3": SEGMENT_TO_UD3.get(segment, "Top"),
        "ud4": "FPA_Reporting", "ud5": "Top", "ud6": "Top", "ud7": "Top", "ud8": "Top",
    }
    cy = lookup_amount(brucetest, base, scenario_cy, cy_time)
    py = lookup_amount(brucetest, base, scenario_py, py_time)
    return safe_div(cy - py, py)


def calc_swsg(sss: float, unit_pct: float) -> float:
    """SWSG ~ (1+SSS)(1+Unit%) - 1.  Standard sales-weighted growth."""
    return (1.0 + sss) * (1.0 + unit_pct) - 1.0


def calc_aoig(brucetest: dict, segment: str, cy_time: str, py_time: str, scenario_cy: str, scenario_py: str) -> float:
    """AOIG = (AOI_CY / AOI_PY) - 1."""
    base = {
        "entity": "RestaurantBrandsIntlMNGT",
        "account": "AOI",
        "ud1": "LOB_TotalEBITDA_FPA", "ud2": "Top",
        "ud3": SEGMENT_TO_UD3.get(segment, "Top"),
        "ud4": "FPA_Reporting", "ud5": "Top", "ud6": "Top", "ud7": "Top", "ud8": "Top",
    }
    cy = lookup_amount(brucetest, base, scenario_cy, cy_time)
    py = lookup_amount(brucetest, base, scenario_py, py_time)
    return safe_div(cy - py, py)


# UD3 segment-rollup labels -- best-known values from the workbook.
SEGMENT_TO_UD3 = {
    "th-cus":    "TH USC",
    "bk-usc":    "BK USC",
    "plk-usc":   "PLK USC",
    "fhs-usc":   "FHS USC",
    "rbi-intl":  "RBI_International",
    "rbi-level": "RBI Organization",
}


# ---------- main shape build --------------------------------------------

def empty_period_block() -> dict[str, float]:
    return {
        "actual": 0, "le": 0, "budget": 0, "py": 0,
        "scenario5": 0, "scenario6": 0, "scenario7": 0,
        "scenario8": 0, "scenario9": 0, "scenario10": 0,
        "vsLE": 0, "vsBudget": 0, "vsPY": 0,
    }


def populate_block(brucetest: dict, mrow: dict[str, str], period_cfg: dict[str, Any], period_key: str) -> dict[str, float]:
    block = empty_period_block()
    pdef = period_cfg["periods"][period_key]
    for col, sdef in period_cfg["scenarios"].items():
        time_member = pdef[sdef["side"]]
        block[col] = lookup_amount(brucetest, mrow, sdef["os_scenario"], time_member)
    block["vsLE"]     = block["actual"] - block["le"]
    block["vsBudget"] = block["actual"] - block["budget"]
    block["vsPY"]     = block["actual"] - block["py"]
    return block


def populate_calculated_kpi(brucetest: dict, segment: str, metric: str, period_cfg: dict[str, Any], period_key: str) -> dict[str, float]:
    block = empty_period_block()
    pdef = period_cfg["periods"][period_key]
    cy_time = pdef["cy"]
    py_time = pdef["py"]

    for col, sdef in period_cfg["scenarios"].items():
        scen_cy = sdef["os_scenario"]
        side    = sdef["side"]
        # For PY column we use the PY year for both sides; for the others we
        # use cy time for "current value" and py time for the prior reference.
        if metric == "Unit %":
            value = calc_unit_pct(
                brucetest, segment,
                cy_time if side == "cy" else py_time,
                py_time,
                scen_cy, "Actual",
            )
        elif metric == "SWSG":
            sss_block = populate_block(brucetest, _kpi_lookup(segment, "SSS"), period_cfg, period_key)
            unit_block = populate_calculated_kpi(brucetest, segment, "Unit %", period_cfg, period_key)
            value = calc_swsg(sss_block[col], unit_block[col])
        elif metric == "AOIG":
            value = calc_aoig(
                brucetest, segment,
                cy_time if side == "cy" else py_time,
                py_time,
                scen_cy, "Actual",
            )
        else:
            value = 0.0
        block[col] = value

    block["vsLE"]     = block["actual"] - block["le"]
    block["vsBudget"] = block["actual"] - block["budget"]
    block["vsPY"]     = block["actual"] - block["py"]
    return block


# Cache: built lazily after mapping is loaded.
_KPI_LOOKUP_CACHE: dict[tuple[str, str], dict[str, str]] = {}
_MAPPING_BY_KEY: dict[tuple[str, str, str], dict[str, str]] = {}


def _kpi_lookup(segment: str, metric: str) -> dict[str, str]:
    """Return a synthetic mapping row for a KPI metric, used by SWSG calc."""
    key = (segment, metric)
    if key in _KPI_LOOKUP_CACHE:
        return _KPI_LOOKUP_CACHE[key]
    row = _MAPPING_BY_KEY.get((segment, "kpi", metric)) or {
        "entity": "RestaurantBrandsIntlMNGT",
        "account": "ReportedSystemCompSalesCY",
        "ud1": "Top", "ud2": "Top",
        "ud3": SEGMENT_TO_UD3.get(segment, "Top"),
        "ud4": "FPA_Reporting", "ud5": "Top", "ud6": "Top", "ud7": "Top", "ud8": "Top",
    }
    _KPI_LOOKUP_CACHE[key] = row
    return row


def build_dashboard(brucetest: dict, mapping: list[dict[str, str]], period_cfg: dict[str, Any]) -> dict:
    # Index the mapping by (segment, section, name) for fast lookup.
    for m in mapping:
        _MAPPING_BY_KEY[(m["segment"], m["section"], m["dashboard_name"])] = m

    # Group by segment.
    segments_out = []
    for seg_id, seg_name, short, currency in SEGMENT_META:
        seg_obj = {
            "id": seg_id,
            "name": seg_name,
            "shortName": short,
            "currency": currency,
            "kpi": [],
            "kpiDetails": [],
            "flowthrough": {"drivers": []},
            "pnl": {"lines": []},
        }

        # KPI + KPIDetails: same metric set, both sections.
        kpi_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "kpi"]
        for m in kpi_rows:
            metric = m["dashboard_name"]
            entry = {"metric": metric}
            if m["match_status"] == "CALCULATED":
                for pk in ("month", "quarter", "ytd"):
                    entry[pk] = populate_calculated_kpi(brucetest, seg_id, metric, period_cfg, pk)
            else:
                for pk in ("month", "quarter", "ytd"):
                    entry[pk] = populate_block(brucetest, m, period_cfg, pk)
            seg_obj["kpi"].append(entry)

        # kpiDetails currently mirrors kpi block-for-block.  In the embedded
        # dashboard they were empty; we populate them so the section is no
        # longer blank.  Refine later if you want entity-level breakdowns.
        for kpi_entry in seg_obj["kpi"]:
            seg_obj["kpiDetails"].append(json.loads(json.dumps(kpi_entry)))

        # Flowthrough drivers.
        ft_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "flowthrough"]
        for m in ft_rows:
            entry = {"line": m["dashboard_name"]}
            for pk in ("month", "quarter", "ytd"):
                entry[pk] = populate_block(brucetest, m, period_cfg, pk)
            seg_obj["flowthrough"]["drivers"].append(entry)

        # PnL lines.
        pnl_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "pnl"]
        for m in pnl_rows:
            entry = {"line": m["dashboard_name"]}
            for pk in ("month", "quarter", "ytd"):
                entry[pk] = populate_block(brucetest, m, period_cfg, pk)
            seg_obj["pnl"]["lines"].append(entry)

        segments_out.append(seg_obj)

    return {
        "metadata": {
            "sourceFile": "Brand FPA Close Master_vMar26_vTest2.xlsx",
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "periodLabel": period_cfg.get("label", ""),
            "periods": ["month", "quarter", "ytd"],
            "notes": [
                "Generated from brucetest by transform.py.",
                f"Period config: month {period_cfg['periods']['month']}, "
                f"quarter {period_cfg['periods']['quarter']}, "
                f"ytd {period_cfg['periods']['ytd']}.",
            ],
        },
        "segments": segments_out,
    }


# ---------- entry point --------------------------------------------------

def main() -> None:
    print(f"transform.py: ROOT = {ROOT}")

    cols, rows = load_brucetest()
    print(f"  brucetest rows : {len(rows):,}")

    brucetest = index_brucetest(cols, rows)
    print(f"  brucetest cells indexed: {len(brucetest):,}")

    with MAPPING_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        mapping = list(csv.DictReader(f))
    print(f"  mapping rows   : {len(mapping)}")

    period_cfg = load_period_config()
    print(f"  period label   : {period_cfg.get('label')}")

    payload = build_dashboard(brucetest, mapping, period_cfg)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {OUTPUT_PATH}")

    # tiny meta file for the dashboard footer
    with META_PATH.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "fetchedAt": datetime.now(timezone.utc).isoformat(),
                "sourceTable": "brucetest",
                "rowsRead": len(rows),
                "periodLabel": period_cfg.get("label"),
            },
            f,
        )
    print(f"  wrote {META_PATH}")


if __name__ == "__main__":
    main()
