import os
import json
import time
import yaml
import requests
import datetime as dt
from typing import Dict, Any, Tuple, Optional
from dotenv import load_dotenv
load_dotenv()

def fred_observations(base_url: str, api_key: str, series_id: str, limit: int = 400) -> Dict[str, Any]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit
    }
    r = requests.get(base_url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def latest_value(obs_json: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    for row in obs_json.get("observations", []):
        v = row.get("value")
        d = row.get("date")
        if v in (None, ".") or d is None:
            continue
        try:
            return d, float(v)
        except ValueError:
            continue
    return None, None

def delta_approx(obs_json: Dict[str, Any], n_obs_back: int = 5) -> Optional[float]:
    vals = []
    for row in obs_json.get("observations", []):
        v = row.get("value")
        if v in (None, "."):
            continue
        try:
            vals.append(float(v))
        except ValueError:
            continue
        if len(vals) >= (n_obs_back + 1):
            break
    if len(vals) >= (n_obs_back + 1):
        return vals[0] - vals[n_obs_back]
    return None

def gdelt_doc_count(doc_api_base: str, q: str, startdatetime: str, enddatetime: str) -> Optional[int]:
    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "startdatetime": startdatetime,
        "enddatetime": enddatetime,
        "maxrecords": 1,
        "sourcelang": "english"
    }
    r = requests.get(doc_api_base, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("totalarticles")

def render_markdown(pack: Dict[str, Any]) -> str:
    meta = pack["meta"]
    lines = []
    lines.append(f"# Macro Context Pack — {meta['as_of_date_local']}")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- as_of_utc: {meta['as_of_utc']}")
    lines.append(f"- timezone_label: {meta['timezone_label']}")
    lines.append("")
    for gname, items in pack["groups"].items():
        lines.append(f"## {gname}")
        lines.append("| Series | Latest date | Latest value | Δ (approx, 5 obs) |")
        lines.append("|---|---:|---:|---:|")
        for it in items:
            if "error" in it:
                lines.append(f"| {it['label']} ({it['series_id']}) |  |  |  |")
                continue
            lv = it.get("latest_value")
            dv = it.get("delta_approx_5obs")
            lines.append(f"| {it['label']} ({it['series_id']}) | {it.get('latest_date','')} | {'' if lv is None else lv} | {'' if dv is None else dv} |")
        lines.append("")
    if "news_pulse" in pack:
        lines.append("## News / Trend Pulse (GDELT)")
        lines.append("| Topic | Lookback (days) | Total articles |")
        lines.append("|---|---:|---:|")
        for row in pack["news_pulse"]:
            lines.append(f"| {row.get('name')} | {row.get('lookback_days')} | {row.get('total_articles','')} |")
        lines.append("")
        lines.append("> Volume/presence only (not sentiment, not truth).")
    lines.append("## Data Quality")
    if pack.get("missing_fields"):
        lines.append("### Missing or errored fields")
        for m in pack["missing_fields"]:
            lines.append(f"- {m}")
    else:
        lines.append("- No missing fields detected.")
    return "\n".join(lines)

def main():
    with open("macro_config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    fred_cfg = cfg["fred"]
    api_key = os.getenv(fred_cfg["api_key_env"], "").strip()
    if not api_key:
        raise SystemExit(f"Missing FRED API key env var: {fred_cfg['api_key_env']}")

    as_of_date_local = dt.date.today().isoformat()
    as_of_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    pack = {
        "meta": {
            "project": cfg.get("project"),
            "version": cfg.get("version"),
            "as_of_date_local": as_of_date_local,
            "as_of_utc": as_of_utc,
            "timezone_label": cfg.get("as_of_timezone", "America/New_York"),
            "sources": {
                "fred_base_url": fred_cfg["base_url"],
                "gdelt_doc_api_base": cfg.get("gdelt", {}).get("doc_api_base")
            }
        },
        "groups": {},
        "missing_fields": []
    }

    for gname, series_list in cfg.get("series_groups", {}).items():
        pack["groups"][gname] = []
        for s in series_list:
            sid = s["id"]
            label = s.get("label", sid)
            try:
                raw = fred_observations(fred_cfg["base_url"], api_key, sid)
                d, v = latest_value(raw)
                dv = delta_approx(raw, 5)
                if v is None:
                    pack["missing_fields"].append(f"FRED:{sid}")
                pack["groups"][gname].append({
                    "series_id": sid,
                    "label": label,
                    "latest_date": d,
                    "latest_value": v,
                    "delta_approx_5obs": dv,
                    "freq_hint": s.get("freq_hint")
                })
                time.sleep(0.15)
            except Exception as e:
                pack["missing_fields"].append(f"FRED:{sid} (error)")
                pack["groups"][gname].append({"series_id": sid, "label": label, "error": str(e)})

    gd = cfg.get("gdelt", {})
    if gd.get("enabled", False):
        lookback = int(gd.get("lookback_days", 3))
        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=lookback)
        fmt = "%Y%m%d%H%M%S"
        startdt = start.strftime(fmt)
        enddt = end.strftime(fmt)
        pulse = []
        for qobj in gd.get("queries", []):
            name = qobj.get("name", "topic")
            q = qobj.get("q", "")
            if not q:
                continue
            try:
                total = gdelt_doc_count(gd["doc_api_base"], q, startdt, enddt)
                pulse.append({"name": name, "lookback_days": lookback, "total_articles": total})
                time.sleep(0.15)
            except Exception as e:
                pulse.append({"name": name, "lookback_days": lookback, "error": str(e)})
        pack["news_pulse"] = pulse

    out_dir = cfg.get("output", {}).get("out_dir", "./outputs")
    os.makedirs(out_dir, exist_ok=True)

    json_path = os.path.join(out_dir, f"macro_context_{as_of_date_local}.json")
    md_path = os.path.join(out_dir, f"macro_context_{as_of_date_local}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(pack, f, indent=2)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(render_markdown(pack))

    print("Wrote:")
    print(" -", md_path)
    print(" -", json_path)

if __name__ == "__main__":
    main()
