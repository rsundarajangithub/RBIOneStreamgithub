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
        # ppy is the prior-prior-year reference -- needed only for the
        # ``py`` column of YoY-ratio metrics (AOIG, Unit %).  SSS/SST do
        # not need it because the comp accounts for both years live at
        # the same time member.
        "month":   {"cy": "2026M3", "py": "2025M3", "ppy": "2024M3"},
        "quarter": {"cy": "2026Q1", "py": "2025Q1", "ppy": "2024Q1"},
        "ytd":     {"cy": "2026Q1", "py": "2025Q1", "ppy": "2024Q1"},
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


def load_brucetest() -> list[dict[str, Any]]:
    """Load the OneStream SqlAdapter response.

    The DataProvider returns ``{"ResultsTable":[ {col:val, ...}, ... ]}``.
    Older synthetic test payloads used ``{"Tables":[{"Columns":[...],"Rows":[[...]]}]}``;
    we still accept that shape for local development.
    Returns a list of row dicts keyed by lower-case column name.
    """
    if not BRUCETEST_PATH.exists():
        sys.stderr.write(f"FATAL: {BRUCETEST_PATH} missing.\n")
        sys.exit(1)

    # utf-8-sig tolerates a BOM if some upstream tool wrote one.
    with BRUCETEST_PATH.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)

    # Shape A: real OneStream response.
    rows_raw = payload.get("ResultsTable")
    if isinstance(rows_raw, list):
        return [{ (k or "").lower(): v for k, v in r.items() } for r in rows_raw]

    # Shape B: dataset-style (legacy synthetic).
    tables = payload.get("Tables") or payload.get("tables") or []
    if tables:
        t = tables[0]
        cols = [(c.get("ColumnName") or c.get("Name") or "").lower()
                for c in t.get("Columns", [])]
        return [dict(zip(cols, r)) for r in t.get("Rows", [])]

    sys.stderr.write(
        "FATAL: brucetest_export.json has no ResultsTable or Tables. "
        f"Top-level keys: {list(payload.keys())[:10]}\n"
    )
    sys.exit(1)


def index_brucetest(rows: list[dict[str, Any]]) -> dict:
    """Build a lookup keyed on the same fields the mapping uses."""
    def s(row: dict[str, Any], name: str) -> str:
        v = row.get(name)
        return "" if v is None else str(v)

    def numeric(row: dict[str, Any], name: str) -> float:
        v = row.get(name)
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    # brucetest has duplicate cube keys because XFC_INTERSECTION_INPUT
    # frequently has many input rows that map to the same OneStream cell
    # (different dashboard labels pointing at the same intersection).
    # When that happens we keep whichever copy is *farther from zero*
    # -- a non-zero value beats a zero value, and a real number beats
    # whatever the last extraction happened to write.
    idx: dict[tuple, float] = {}
    for row in rows:
        key = (
            s(row, "entity"),
            s(row, "scenario"),
            s(row, "time_member"),
            s(row, "os_account"),
            s(row, "ud1"),
            s(row, "ud2"),
            s(row, "ud3"),
            s(row, "ud4"),
            s(row, "ud5"),
            s(row, "ud6"),
            s(row, "ud7"),
            s(row, "ud8"),
        )
        amt = numeric(row, "amount")
        prev = idx.get(key)
        if prev is None or abs(amt) > abs(prev):
            idx[key] = amt

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

# UD3 segment-rollup labels -- verified against actual brucetest data.
# Used by the calculated KPI metrics and as a fallback when a mapping
# row's UD3 returns 0.
SEGMENT_TO_UD3 = {
    "th-cus":    "TH C&US",
    "bk-usc":    "BK USC",
    "plk-usc":   "PLK USC",
    "fhs-usc":   "FHS USC",
    "rbi-intl":  "RBI_International",
    "rbi-level": "RBI Organization",
}


def _seg_base(segment: str, account: str, ud1: str = "Top", ud2: str = "Top") -> dict[str, str]:
    """Synthesise a mapping-style row for a segment-rollup intersection."""
    return {
        "entity":  "RestaurantBrandsIntlMNGT",
        "account": account,
        "ud1": ud1, "ud2": ud2,
        "ud3": SEGMENT_TO_UD3.get(segment, "Top"),
        "ud4": "FPA_Reporting",
        "ud5": "Top", "ud6": "Top", "ud7": "Top", "ud8": "Top",
    }


def calc_sss_at(brucetest: dict, segment: str, scenario: str, time_member: str) -> float:
    """SSS at a given (scenario, time) =
       (ReportedSystemCompSalesCY - ReportedSystemCompSalesPY) / ReportedSystemCompSalesPY."""
    cy_row = _seg_base(segment, "ReportedSystemCompSalesCY")
    py_row = _seg_base(segment, "ReportedSystemCompSalesPY")
    cy = lookup_amount(brucetest, cy_row, scenario, time_member)
    py = lookup_amount(brucetest, py_row, scenario, time_member)
    return safe_div(cy - py, py)


def calc_sst_at(brucetest: dict, segment: str, scenario: str, time_member: str) -> float:
    """SST at a given (scenario, time) using comp traffic accounts."""
    cy_row = _seg_base(segment, "ReportedSystemCompTrafficCY")
    py_row = _seg_base(segment, "ReportedSystemCompTrafficPY")
    cy = lookup_amount(brucetest, cy_row, scenario, time_member)
    py = lookup_amount(brucetest, py_row, scenario, time_member)
    return safe_div(cy - py, py)


def calc_aoig_at(brucetest: dict, segment: str, scenario: str, cy_time: str, prior_time: str) -> float:
    """AOIG at (scenario, cy_time) vs Actual at prior_time."""
    base = _seg_base(segment, "AOI", ud1="LOB_TotalEBITDA_FPA")
    cy = lookup_amount(brucetest, base, scenario, cy_time)
    py = lookup_amount(brucetest, base, "Actual", prior_time)
    return safe_div(cy - py, py)


def calc_unit_pct_at(brucetest: dict, segment: str, scenario: str, cy_time: str, prior_time: str) -> float:
    """Unit % at (scenario, cy_time) vs Actual at prior_time."""
    base = _seg_base(segment, "SystemStoreCount")
    cy = lookup_amount(brucetest, base, scenario, cy_time)
    py = lookup_amount(brucetest, base, "Actual", prior_time)
    return safe_div(cy - py, py)


def calc_swsg(sss: float, unit_pct: float) -> float:
    """SWSG ~ (1+SSS)(1+Unit%) - 1.  Standard sales-weighted growth."""
    return (1.0 + sss) * (1.0 + unit_pct) - 1.0


# ---------- metric-type catalogue ---------------------------------------
# Mirrors RBI-Close-Dashboard.html inferKpiType / inferPnlType so we
# scale and shape values exactly the way the renderer wants them.

KPI_PERCENT_METRICS = {"SST", "SSS", "Unit %", "SWSG", "AOIG"}
KPI_NUMBER_METRICS  = {"NRG"}
PNL_PERCENT_LINES   = {"Royalty Rate %"}


def kpi_type(metric: str) -> str:
    if metric in KPI_PERCENT_METRICS:
        return "percentage"
    if metric in KPI_NUMBER_METRICS:
        return "number"
    return "currency"


def pnl_type(line: str) -> str:
    return "percentage" if line in PNL_PERCENT_LINES else "currency"


_SCEN_COLS = ("actual", "le", "budget", "py",
              "scenario5", "scenario6", "scenario7",
              "scenario8", "scenario9", "scenario10")

_DOLLARS_PER_M = 1_000_000.0


def scale_block_to_millions(block: dict[str, float]) -> dict[str, float]:
    """Convert a raw-dollar period block to $M and recompute variances."""
    for col in _SCEN_COLS:
        block[col] = (block.get(col) or 0.0) / _DOLLARS_PER_M
    block["vsLE"]     = block["actual"] - block["le"]
    block["vsBudget"] = block["actual"] - block["budget"]
    block["vsPY"]     = block["actual"] - block["py"]
    return block


def recompute_variances(block: dict[str, float]) -> dict[str, float]:
    """Refresh vs* fields after scenario columns have been touched."""
    block["vsLE"]     = (block.get("actual") or 0.0) - (block.get("le")     or 0.0)
    block["vsBudget"] = (block.get("actual") or 0.0) - (block.get("budget") or 0.0)
    block["vsPY"]     = (block.get("actual") or 0.0) - (block.get("py")     or 0.0)
    return block


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
        amt = lookup_amount(brucetest, mrow, sdef["os_scenario"], time_member)
        # Quarter / YTD cells are often empty in the cube even when the
        # constituent months are populated.  When that happens we sum the
        # months that make up the period.  This is a heuristic; flip it
        # off via the period.json override if the cube ever rolls things
        # up properly on its own.
        if amt == 0 and time_member.endswith(("Q1", "Q2", "Q3", "Q4")):
            amt = sum_months_for_quarter(brucetest, mrow, sdef["os_scenario"], time_member)
        block[col] = amt
    block["vsLE"]     = block["actual"] - block["le"]
    block["vsBudget"] = block["actual"] - block["budget"]
    block["vsPY"]     = block["actual"] - block["py"]
    return block


_QUARTER_MONTHS = {"Q1": (1, 2, 3), "Q2": (4, 5, 6), "Q3": (7, 8, 9), "Q4": (10, 11, 12)}


def sum_months_for_quarter(brucetest: dict, mrow: dict[str, str], scenario: str, quarter_member: str) -> float:
    """Reconstruct a quarter total from monthly cube cells -- e.g.
    ``2026Q1`` -> sum of ``2026M1 + 2026M2 + 2026M3``."""
    year = quarter_member[:4]
    qkey = quarter_member[-2:]
    months = _QUARTER_MONTHS.get(qkey)
    if not months:
        return 0.0
    total = 0.0
    for m in months:
        total += lookup_amount(brucetest, mrow, scenario, f"{year}M{m}")
    return total


def populate_calculated_kpi(brucetest: dict, segment: str, metric: str,
                            period_cfg: dict[str, Any], period_key: str) -> dict[str, float]:
    """Build a period block where every scenario column carries that
    scenario's own SSS/SST/Unit %/SWSG/AOIG ratio.

    For ``actual``/``le``/``budget`` columns we compute the metric at
    (that scenario, ``cy_time``) referenced against ``py_time``.
    For the ``py`` column we report what the metric WAS in the prior
    period -- i.e. evaluated at (Actual, ``py_time``) referenced
    against ``ppy_time``.
    """
    block = empty_period_block()
    pdef = period_cfg["periods"][period_key]
    cy_time  = pdef["cy"]
    py_time  = pdef["py"]
    ppy_time = pdef.get("ppy") or py_time   # graceful fallback

    for col, sdef in period_cfg["scenarios"].items():
        scen = sdef["os_scenario"]
        side = sdef["side"]
        # Determine the (scenario, time) at which to evaluate the metric
        # *for this column*, and the prior-period reference time used by
        # YoY-ratio metrics (AOIG, Unit %).
        if side == "cy":
            eval_time, prior_time = cy_time, py_time
        else:
            eval_time, prior_time = py_time, ppy_time

        if metric == "SSS":
            value = calc_sss_at(brucetest, segment, scen, eval_time)
        elif metric == "SST":
            value = calc_sst_at(brucetest, segment, scen, eval_time)
        elif metric == "Unit %":
            value = calc_unit_pct_at(brucetest, segment, scen, eval_time, prior_time)
        elif metric == "AOIG":
            value = calc_aoig_at(brucetest, segment, scen, eval_time, prior_time)
        elif metric == "SWSG":
            sss = calc_sss_at(brucetest, segment, scen, eval_time)
            unt = calc_unit_pct_at(brucetest, segment, scen, eval_time, prior_time)
            value = calc_swsg(sss, unt)
        else:
            value = 0.0

        block[col] = value

    return recompute_variances(block)


# ---------- baseline (snapshot from the original embedded dashboard) ---
# Used as a fallback for the flowthrough drivers, where the workbook math
# (per-driver "flowthrough" decimal contribution) is not yet replicated
# in the transformer.  Once the formula is encoded the baseline can be
# retired.

BASELINE_PATH = CONFIG_DIR / "dashboard_baseline.json"


def load_baseline() -> dict | None:
    if not BASELINE_PATH.exists():
        return None
    with BASELINE_PATH.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def baseline_segment(baseline: dict | None, seg_id: str) -> dict | None:
    if not baseline:
        return None
    for s in baseline.get("segments", []):
        if s.get("id") == seg_id:
            return s
    return None


def baseline_flow_driver(seg_baseline: dict | None, line: str) -> dict | None:
    if not seg_baseline:
        return None
    for d in (seg_baseline.get("flowthrough") or {}).get("drivers", []):
        if d.get("line") == line:
            return d
    return None


def baseline_pnl_periodLabels(seg_baseline: dict | None) -> dict:
    return ((seg_baseline or {}).get("pnl") or {}).get("periodLabels") or {}


def baseline_kpi_details(seg_baseline: dict | None) -> list:
    return (seg_baseline or {}).get("kpiDetails") or []


def empty_flow_block() -> dict[str, Any]:
    return {"flowthrough": None, "yoy": None, "yoyText": None}


def build_dashboard(brucetest: dict, mapping: list[dict[str, str]],
                    period_cfg: dict[str, Any], baseline: dict | None) -> dict:
    segments_out = []
    for seg_id, seg_name, short, currency in SEGMENT_META:
        seg_baseline = baseline_segment(baseline, seg_id)
        seg_obj = {
            "id": seg_id,
            "name": seg_name,
            "shortName": short,
            "currency": currency,
            "kpi": [],
            "kpiDetails": baseline_kpi_details(seg_baseline),
            "flowthrough": {"drivers": []},
            "pnl": {"lines": [], "periodLabels": baseline_pnl_periodLabels(seg_baseline)},
        }

        # ---------------- KPI ----------------
        kpi_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "kpi"]
        for m in kpi_rows:
            metric  = m["dashboard_name"]
            mtype   = kpi_type(metric)
            entry   = {"metric": metric}
            for pk in ("month", "quarter", "ytd"):
                if mtype == "percentage":
                    # All percentage KPIs are computed (SSS/SST included).
                    entry[pk] = populate_calculated_kpi(brucetest, seg_id, metric, period_cfg, pk)
                else:
                    block = populate_block(brucetest, m, period_cfg, pk)
                    if mtype == "currency":
                        scale_block_to_millions(block)
                    else:
                        recompute_variances(block)
                    entry[pk] = block
            seg_obj["kpi"].append(entry)

        # kpiDetails: keep the original snapshot shape so the detail panes
        # continue to render exactly as they did in the embedded dashboard.
        # If/when we have line-level brucetest coverage, replace this.

        # ---------------- Flowthrough drivers ----------------
        # Use the baseline values verbatim.  Driver-level "flowthrough %"
        # is computed by Excel formulas in the workbook that we have not
        # yet replicated; emitting raw cube dollars here would render as
        # nonsense.  Listing the baseline at least lets the driver ladder
        # render correctly until the formula is encoded.
        if seg_baseline:
            for d in (seg_baseline.get("flowthrough") or {}).get("drivers", []):
                seg_obj["flowthrough"]["drivers"].append(json.loads(json.dumps(d)))
        else:
            ft_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "flowthrough"]
            for m in ft_rows:
                entry = {"line": m["dashboard_name"]}
                for pk in ("month", "quarter", "ytd"):
                    entry[pk] = empty_flow_block()
                seg_obj["flowthrough"]["drivers"].append(entry)

        # ---------------- PnL lines ----------------
        pnl_rows = [m for m in mapping if m["segment"] == seg_id and m["section"] == "pnl"]
        for m in pnl_rows:
            line  = m["dashboard_name"]
            ltype = pnl_type(line)
            entry = {"line": line}
            for pk in ("month", "quarter", "ytd"):
                block = populate_block(brucetest, m, period_cfg, pk)
                if ltype == "currency":
                    scale_block_to_millions(block)
                else:
                    recompute_variances(block)
                entry[pk] = block
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
                "KPI/PnL values computed live from the cube (decimals for %, $M for currency).",
                "Flowthrough drivers and KPI detail tables are sourced from the workbook baseline (config/dashboard_baseline.json) until their formulas are replicated.",
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

    rows = load_brucetest()
    print(f"  brucetest rows : {len(rows):,}")

    brucetest = index_brucetest(rows)
    print(f"  brucetest cells indexed: {len(brucetest):,}")

    with MAPPING_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        mapping = list(csv.DictReader(f))
    print(f"  mapping rows   : {len(mapping)}")

    period_cfg = load_period_config()
    print(f"  period label   : {period_cfg.get('label')}")

    baseline = load_baseline()
    print(f"  baseline       : {'loaded' if baseline else 'none (flowthrough will be empty)'}")

    payload = build_dashboard(brucetest, mapping, period_cfg, baseline)

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
