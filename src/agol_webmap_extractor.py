#!/usr/bin/env python3
"""
Export extractable layers from an ArcGIS Online / Enterprise web map to a local File Geodatabase.

Highlights
----------
- Reads a web map JSON item and discovers operational layers recursively, including group layers.
- Attempts to export each feature layer / table to a local FGDB.
- Skips failures and continues unless you choose to stop on first error.
- Produces detailed logging and CSV/TXT summaries of successes and failures.
- Prefers ArcPy export first, then falls back to ArcGIS API for Python query + SEDF export.

Interactive mode (recommended)
------------------------------
    python src/agol_webmap_extractor.py

Command-line mode
-----------------
    python src/agol_webmap_extractor.py \
        --portal https://www.arcgis.com \
        --webmap-id YOUR_WEBMAP_ITEM_ID \
        --username YOUR_USERNAME \
        --output-folder C:\\temp\\wm_export \
        --gdb-name webmap_export.gdb \
        --debug
"""

from __future__ import annotations

import argparse
import csv
import getpass
import logging
import os
import re
import sys
import time
import traceback
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import arcpy  # type: ignore
except Exception:
    arcpy = None

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None

try:
    from arcgis.features import FeatureLayer  # type: ignore
    from arcgis.gis import GIS  # type: ignore
except Exception:
    FeatureLayer = None
    GIS = None


@dataclass
class LayerCandidate:
    path: str
    title: str
    layer_type: str
    url: str
    item_id: str = ""
    service_type: str = ""
    layer_id: Optional[int] = None
    is_table_hint: bool = False


@dataclass
class ExportResult:
    order: int
    path: str
    title: str
    url: str
    layer_type: str
    output_name: str
    exported_object: str
    status: str
    method_used: str
    record_count: Optional[int]
    elapsed_seconds: float
    notes: str


INVALID_FGDB_CHARS = re.compile(r"[^A-Za-z0-9_]+")
RESERVED_NAMES = {
    "add", "alter", "and", "as", "asc", "between", "by", "column", "create",
    "date", "delete", "desc", "drop", "from", "group", "in", "insert", "into",
    "is", "like", "not", "null", "or", "order", "select", "set", "table", "update",
    "where",
}


def setup_logging(output_folder: str, debug: bool) -> str:
    os.makedirs(output_folder, exist_ok=True)
    log_path = os.path.join(output_folder, "webmap_export.log")
    level = logging.DEBUG if debug else logging.INFO

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return log_path


def sanitize_name(name: str, max_len: int = 60) -> str:
    name = name.strip().replace(" ", "_")
    name = INVALID_FGDB_CHARS.sub("_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "layer"
    if name[0].isdigit():
        name = f"L_{name}"
    if name.lower() in RESERVED_NAMES:
        name = f"{name}_fc"
    return name[:max_len]


def unique_name(base: str, existing: set[str], max_len: int = 60) -> str:
    base = sanitize_name(base, max_len=max_len)
    if base.lower() not in existing:
        existing.add(base.lower())
        return base
    i = 1
    while True:
        suffix = f"_{i}"
        candidate = f"{base[: max_len - len(suffix)]}{suffix}"
        if candidate.lower() not in existing:
            existing.add(candidate.lower())
            return candidate
        i += 1


def is_feature_like_url(url: str) -> bool:
    u = (url or "").lower()
    return "/featureserver" in u or "/mapserver" in u


def infer_service_type(url: str) -> str:
    if not url:
        return ""
    u = url.lower()
    if "/featureserver/" in u or u.endswith("/featureserver"):
        return "FeatureServer"
    if "/mapserver/" in u or u.endswith("/mapserver"):
        return "MapServer"
    return ""


def extract_layer_id(url: str) -> Optional[int]:
    if not url:
        return None
    match = re.search(r"/(?:FeatureServer|MapServer)/(\d+)(?:/)?$", url, re.IGNORECASE)
    return int(match.group(1)) if match else None


def likely_reason(exc_text: str) -> str:
    text = (exc_text or "").lower()
    rules = [
        ("token", "Authentication/token issue or expired sign-in."),
        ("not authorized", "No permission to access this layer/service."),
        ("permission", "Insufficient permissions for this layer/service."),
        ("privilege", "Account lacks required privilege."),
        ("export", "Export disabled or data download blocked by owner/admin."),
        ("sync", "Replica/sync requirement not met for that export path."),
        ("schema lock", "Schema lock or destination FGDB lock encountered."),
        ("cannot open", "Service or destination could not be opened."),
        ("service is unavailable", "Service unavailable or temporarily down."),
        ("timeout", "Request timed out; dataset may be large or service slow."),
        ("maxrecordcount", "Large layer; pagination/transfer limit may have been hit."),
        ("supportsquery", "Layer likely not queryable."),
        ("query", "Query operation failed or is not allowed."),
        ("geometry", "Geometry issue or unsupported geometry type."),
        ("memory", "Memory/resource limit reached while exporting."),
        ("unsupported", "Unsupported layer type for this script."),
        ("dataset does not exist", "Referenced sublayer/service no longer exists."),
        ("ssl", "SSL/certificate problem."),
        ("proxy", "Proxy/network issue."),
    ]
    for key, value in rules:
        if key in text:
            return value
    return "See raw error in notes/log for the best clue."


def prompt_text(label: str, default: Optional[str] = None, required: bool = False) -> str:
    while True:
        suffix = f" [{default}]" if default not in (None, "") else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default not in (None, ""):
            return default
        if not required:
            return ""
        print("This value is required. Please enter a value.")


def prompt_yes_no(label: str, default: bool = False) -> bool:
    default_txt = "Y/n" if default else "y/N"
    while True:
        value = input(f"{label} [{default_txt}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please enter y or n.")


def collect_interactive_inputs(args: argparse.Namespace) -> argparse.Namespace:
    print("\nArcGIS Web Map to FGDB Export")
    print("-" * 34)

    args.portal = args.portal or prompt_text("Portal URL", default="https://www.arcgis.com", required=True)
    args.webmap_id = args.webmap_id or prompt_text("Web Map Item ID", required=True)
    args.output_folder = args.output_folder or prompt_text("Local output folder", required=True)
    args.gdb_name = args.gdb_name or prompt_text("Output FGDB name", default="webmap_export.gdb", required=True)

    use_signin = prompt_yes_no("Does this web map require sign-in?", default=True)
    if use_signin:
        args.username = args.username or prompt_text("Username", required=True)
        if not args.password:
            args.password = getpass.getpass("Password: ")
    else:
        args.username = ""
        args.password = ""

    if not args.debug:
        args.debug = prompt_yes_no("Enable debug logging?", default=True)
    if not args.stop_on_error:
        args.stop_on_error = prompt_yes_no("Stop on first layer error?", default=False)

    print("\nInputs captured. Starting export...\n")
    return args


def flatten_operational_layers(layer_defs: Iterable[Dict[str, Any]], parent_path: str = "") -> List[LayerCandidate]:
    out: List[LayerCandidate] = []
    for idx, lyr in enumerate(layer_defs or []):
        title = lyr.get("title") or lyr.get("id") or f"layer_{idx}"
        path = f"{parent_path}/{title}" if parent_path else title
        layer_type = lyr.get("layerType") or lyr.get("type") or "Unknown"
        url = lyr.get("url") or lyr.get("layerUrl") or ""
        item_id = lyr.get("itemId") or lyr.get("layerItemId") or ""

        sublayers = lyr.get("layers") or lyr.get("featureCollection", {}).get("layers") or []
        if sublayers and not url:
            out.extend(flatten_operational_layers(sublayers, parent_path=path))

        if not url:
            continue

        if is_feature_like_url(url):
            out.append(
                LayerCandidate(
                    path=path,
                    title=title,
                    layer_type=layer_type,
                    url=url,
                    item_id=item_id,
                    service_type=infer_service_type(url),
                    layer_id=extract_layer_id(url),
                    is_table_hint=False,
                )
            )
    return out


def get_webmap_json(gis: Any, webmap_id: str) -> Tuple[Any, Dict[str, Any]]:
    item = gis.content.get(webmap_id)
    if item is None:
        raise RuntimeError(f"Web map item not found: {webmap_id}")
    if str(item.type).lower() != "web map":
        logging.warning("Item type is '%s', not 'Web Map'. Attempting to read data anyway.", item.type)
    data = item.get_data() or {}
    return item, data


def ensure_fgdb(output_folder: str, gdb_name: str) -> str:
    if not gdb_name.lower().endswith(".gdb"):
        gdb_name += ".gdb"
    gdb_path = os.path.join(output_folder, gdb_name)
    if os.path.exists(gdb_path):
        logging.info("Using existing FGDB: %s", gdb_path)
        return gdb_path
    if arcpy is None:
        raise RuntimeError("ArcPy is required to create a File Geodatabase automatically.")
    logging.info("Creating FGDB: %s", gdb_path)
    arcpy.management.CreateFileGDB(output_folder, os.path.splitext(gdb_name)[0])
    return gdb_path


def describe_remote_layer(url: str) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    try:
        if arcpy is not None:
            desc = arcpy.Describe(url)
            meta["dataType"] = getattr(desc, "dataType", None)
            meta["shapeType"] = getattr(desc, "shapeType", None)
    except Exception:
        pass
    return meta


def try_arcpy_export(url: str, out_path: str) -> Tuple[str, Optional[int], str]:
    if arcpy is None:
        raise RuntimeError("ArcPy not available for primary export path.")

    meta = describe_remote_layer(url)
    logging.debug("ArcPy describe for %s => %s", url, meta)

    try:
        arcpy.conversion.ExportFeatures(url, out_path)
        cnt = int(arcpy.management.GetCount(out_path)[0])
        return out_path, cnt, "arcpy.ExportFeatures"
    except Exception as ex_feat:
        feat_err = str(ex_feat)
        logging.debug("ExportFeatures failed for %s: %s", url, feat_err)
        try:
            arcpy.conversion.ExportTable(url, out_path)
            cnt = int(arcpy.management.GetCount(out_path)[0])
            return out_path, cnt, "arcpy.ExportTable"
        except Exception as ex_tbl:
            raise RuntimeError(
                f"ArcPy export failed. ExportFeatures error: {feat_err} | ExportTable error: {ex_tbl}"
            ) from ex_tbl


def try_sedf_export(url: str, out_path: str, chunk_size: int = 2000) -> Tuple[str, Optional[int], str]:
    if FeatureLayer is None or pd is None:
        raise RuntimeError("ArcGIS API for Python and pandas are required for fallback export path.")

    fl = FeatureLayer(url)
    props = getattr(fl, "properties", {})
    oid_field = getattr(props, "objectIdField", None) or props.get("objectIdField")
    geometry_type = getattr(props, "geometryType", None) or props.get("geometryType")

    try:
        sdf = pd.DataFrame.spatial.from_layer(fl)
        if geometry_type:
            out = sdf.spatial.to_featureclass(location=out_path, overwrite=True)
        else:
            out = sdf.spatial.to_table(location=out_path, overwrite=True)
        return out, len(sdf.index), "SEDF.from_layer"
    except Exception as ex_fast:
        logging.debug("SEDF fast path failed for %s: %s", url, ex_fast)

    if not oid_field:
        raise RuntimeError("Fallback query export requires objectIdField, but none was found.")

    oid_fs = fl.query(where="1=1", return_ids_only=True)
    oid_dict = oid_fs.get("objectIds") if isinstance(oid_fs, dict) else None
    object_ids = sorted(oid_dict or [])
    if not object_ids:
        try:
            empty = fl.query(where="1=2", as_df=True)
            if geometry_type:
                out = empty.spatial.to_featureclass(location=out_path, overwrite=True)
            else:
                out = empty.spatial.to_table(location=out_path, overwrite=True)
            return out, 0, "SEDF.chunked_empty"
        except Exception as ex:
            raise RuntimeError(f"Layer has zero rows and empty schema export failed: {ex}") from ex

    parts = []
    for i in range(0, len(object_ids), chunk_size):
        batch = object_ids[i : i + chunk_size]
        where = f"{oid_field} in ({','.join(map(str, batch))})"
        logging.debug("Querying %s rows %s-%s", url, i + 1, min(i + chunk_size, len(object_ids)))
        part = fl.query(where=where, as_df=True)
        parts.append(part)

    sdf = pd.concat(parts, ignore_index=True)
    if geometry_type:
        out = sdf.spatial.to_featureclass(location=out_path, overwrite=True)
    else:
        out = sdf.spatial.to_table(location=out_path, overwrite=True)
    return out, len(sdf.index), "SEDF.chunked_query"


def export_candidates(candidates: List[LayerCandidate], gdb_path: str, keep_going: bool = True) -> List[ExportResult]:
    results: List[ExportResult] = []
    used_names: set[str] = set()

    for idx, cand in enumerate(candidates, start=1):
        start = time.time()
        output_name = unique_name(cand.title or f"layer_{idx}", used_names)
        out_path = os.path.join(gdb_path, output_name)
        notes = ""
        status = "FAILED"
        method_used = ""
        exported_object = ""
        record_count: Optional[int] = None

        logging.info("[%s/%s] Exporting: %s", idx, len(candidates), cand.path)
        logging.debug("Candidate detail: %s", cand)

        try:
            exported_object, record_count, method_used = try_arcpy_export(cand.url, out_path)
            status = "SUCCESS"
            notes = "Exported via ArcPy."
        except Exception as ex1:
            notes = f"Primary export failed: {ex1}"
            logging.warning("ArcPy export failed for '%s'. Trying fallback. Error: %s", cand.title, ex1)
            logging.debug(traceback.format_exc())
            try:
                exported_object, record_count, method_used = try_sedf_export(cand.url, out_path)
                status = "SUCCESS"
                notes = f"ArcPy failed; fallback succeeded via {method_used}."
            except Exception as ex2:
                raw = f"ArcPy error: {ex1} || Fallback error: {ex2}"
                notes = f"{raw} Likely reason: {likely_reason(raw)}"
                logging.error("Fallback export also failed for '%s': %s", cand.title, ex2)
                logging.debug(traceback.format_exc())
                if not keep_going:
                    raise

        elapsed = round(time.time() - start, 2)
        results.append(
            ExportResult(
                order=idx,
                path=cand.path,
                title=cand.title,
                url=cand.url,
                layer_type=cand.layer_type,
                output_name=output_name,
                exported_object=exported_object,
                status=status,
                method_used=method_used,
                record_count=record_count,
                elapsed_seconds=elapsed,
                notes=notes,
            )
        )
    return results


def write_csv_report(results: List[ExportResult], output_folder: str) -> str:
    csv_path = os.path.join(output_folder, "webmap_export_summary.csv")
    fieldnames = list(asdict(results[0]).keys()) if results else [
        "order", "path", "title", "url", "layer_type", "output_name", "exported_object",
        "status", "method_used", "record_count", "elapsed_seconds", "notes",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(asdict(row))
    return csv_path


def write_txt_report(webmap_title: str, webmap_id: str, results: List[ExportResult], output_folder: str, gdb_path: str) -> str:
    txt_path = os.path.join(output_folder, "webmap_export_summary.txt")
    counts = Counter(r.status for r in results)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("ArcGIS Web Map -> File Geodatabase Export Summary\n")
        f.write("=" * 60 + "\n")
        f.write(f"Web map: {webmap_title}\n")
        f.write(f"Web map ID: {webmap_id}\n")
        f.write(f"Output FGDB: {gdb_path}\n")
        f.write(f"Processed layers: {len(results)}\n")
        f.write(f"Successful: {counts.get('SUCCESS', 0)}\n")
        f.write(f"Failed: {counts.get('FAILED', 0)}\n\n")

        f.write("Per-layer results\n")
        f.write("-" * 60 + "\n")
        for r in results:
            f.write(f"[{r.order}] {r.title} | {r.status} | method={r.method_used or 'n/a'}\n")
            f.write(f"    path: {r.path}\n")
            f.write(f"    url: {r.url}\n")
            f.write(f"    output: {r.exported_object or 'n/a'}\n")
            f.write(f"    count: {r.record_count}\n")
            f.write(f"    notes: {r.notes}\n\n")
    return txt_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export extractable web map layers to a local FGDB.")
    parser.add_argument("--portal", help="Portal URL")
    parser.add_argument("--webmap-id", help="Web map item ID")
    parser.add_argument("--username", help="Portal username")
    parser.add_argument("--password", help="Portal password (omit to be prompted)")
    parser.add_argument("--output-folder", help="Local output folder")
    parser.add_argument("--gdb-name", help="Output File GDB name")
    parser.add_argument("--debug", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop at first layer failure")
    parser.add_argument("--no-prompt", action="store_true", help="Do not prompt interactively; require CLI arguments instead")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    required_missing = [name for name in ["webmap_id", "output_folder"] if not getattr(args, name, None)]
    if required_missing and args.no_prompt:
        print(f"Missing required arguments for non-interactive mode: {', '.join(required_missing)}")
        return 2

    if not args.no_prompt:
        args = collect_interactive_inputs(args)
    else:
        args.portal = args.portal or "https://www.arcgis.com"
        args.gdb_name = args.gdb_name or "webmap_export.gdb"

    log_path = setup_logging(args.output_folder, args.debug)
    logging.info("Starting export job")
    logging.info("Log file: %s", log_path)

    if GIS is None:
        logging.error("ArcGIS API for Python is not available in this Python environment.")
        return 2

    username = args.username
    password = args.password
    if username and not password:
        password = getpass.getpass("Portal password: ")

    try:
        gis = GIS(args.portal, username, password) if username else GIS(args.portal, anonymous=True)
        logging.info("Connected to portal: %s", args.portal)
    except Exception as ex:
        logging.error("Could not connect to portal: %s", ex)
        return 2

    try:
        item, data = get_webmap_json(gis, args.webmap_id)
        webmap_title = getattr(item, "title", args.webmap_id)
        logging.info("Web map: %s (%s)", webmap_title, args.webmap_id)
    except Exception as ex:
        logging.error("Failed to read web map: %s", ex)
        return 2

    try:
        gdb_path = ensure_fgdb(args.output_folder, args.gdb_name)
    except Exception as ex:
        logging.error("Failed to prepare FGDB: %s", ex)
        return 2

    operational_layers = data.get("operationalLayers", [])
    candidates = flatten_operational_layers(operational_layers)
    logging.info("Feature-like layers discovered in web map: %s", len(candidates))

    if not candidates:
        logging.warning("No exportable feature-like layers were found in the web map JSON.")
        return 1

    results = export_candidates(candidates=candidates, gdb_path=gdb_path, keep_going=not args.stop_on_error)

    csv_path = write_csv_report(results, args.output_folder)
    txt_path = write_txt_report(webmap_title, args.webmap_id, results, args.output_folder, gdb_path)

    counts = Counter(r.status for r in results)
    logging.info("Done. Successful=%s, Failed=%s", counts.get("SUCCESS", 0), counts.get("FAILED", 0))
    logging.info("CSV summary: %s", csv_path)
    logging.info("TXT summary: %s", txt_path)
    logging.info("FGDB: %s", gdb_path)
    return 0 if counts.get("SUCCESS", 0) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
