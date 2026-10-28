"""Stream approved nested ZIPs, sample loans, and retain complete histories."""

from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import time
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

from .schema import ORIG_COLUMNS, PERF_COLUMNS


def emit(kind, **values):
    print(json.dumps({"kind": kind, **values}, default=str, allow_nan=False), flush=True)


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def select_loan_ids(ids, seed, numerator, denominator):
    if not isinstance(denominator, int) or denominator <= 0 or not 0 <= numerator <= denominator:
        raise ValueError("Sampling fraction must lie between zero and one")
    threshold = (2**64 * numerator) // denominator
    return {loan for loan in ids if int.from_bytes(
        hashlib.sha256(f"{seed}:{loan}".encode("utf-8")).digest()[:8], "big") < threshold}


def validate_cached_ingestion(repo, config):
    """Bind cached rows to the requested scope, sample, layout and source bytes."""
    repo = Path(repo)
    reports = repo / config["report_dir"]
    provenance = json.loads((reports / "source_fingerprints.json").read_text(encoding="utf-8"))
    summary = json.loads((reports / "ingestion_summary.json").read_text(encoding="utf-8"))
    expected_paths = [(repo / config["raw_dir"] / f"historical_data_{year}.zip").resolve()
                      for year in config["years"]]
    actual_paths = [(repo / source["path"]).resolve() for source in provenance["sources"]]
    if (provenance["sampling"] != config["sample"]
            or provenance["schema_version"] != config["schema_version"]
            or len(actual_paths) != len(expected_paths)
            or set(actual_paths) != set(expected_paths)):
        raise ValueError("Cached ingestion configuration differs; rerun ingestion for the requested settings")
    for source in provenance["sources"]:
        if file_hash(repo / source["path"]) != source["sha256"]:
            raise ValueError(f"Archive changed since ingestion: {source['path']}")
    for filename, expected in summary["parquet_sha256"].items():
        if file_hash(repo / config["output_dir"] / filename) != expected:
            raise ValueError(f"Measured raw Parquet changed: {filename}")
    emit("cached_ingestion_validated", archives=len(actual_paths), sampling=config["sample"],
         schema_version=config["schema_version"])


class HashingReader(io.RawIOBase):
    """Measure the exact uncompressed bytes passed to Arrow's CSV parser."""

    def __init__(self, source):
        super().__init__()
        self.source = source
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.source.read(len(buffer))
        buffer[:len(data)] = data
        self.digest.update(data)
        self.bytes_read += len(data)
        return len(data)


def scan_member(quarter, name, columns, date_column, writer, year, quarter_number, config, selected=None):
    started = time.perf_counter()
    count = retained = 0
    low = high = None
    loan_ids = set() if selected is None else selected
    with quarter.open(name) as compressed, HashingReader(compressed) as measured:
        reader = csv.open_csv(
            measured,
            read_options=csv.ReadOptions(column_names=columns, block_size=config["csv_block_bytes"], use_threads=False),
            parse_options=csv.ParseOptions(delimiter="|", quote_char=False, ignore_empty_lines=False),
            convert_options=csv.ConvertOptions(column_types={name: pa.string() for name in columns}, strings_can_be_null=False),
        )
        for batch in reader:
            count += batch.num_rows
            values = batch.column(date_column)
            limits = pc.min_max(pc.if_else(pc.equal(values, ""), None, values)).as_py()
            if limits["min"] is not None:
                low = limits["min"] if low is None else min(low, limits["min"])
                high = limits["max"] if high is None else max(high, limits["max"])
            if selected is None:
                loan_ids.update(select_loan_ids(batch.column("loan_id").to_pylist(), **config["sample"]))
            mask = pc.is_in(batch.column("loan_id"), value_set=pa.array(sorted(loan_ids), type=pa.string()))
            keep = pa.Table.from_batches([batch.filter(mask)])
            retained += keep.num_rows
            keep = keep.append_column("source_year", pa.array([year] * keep.num_rows, type=pa.int16()))
            keep = keep.append_column("source_quarter", pa.array([f"{year}Q{quarter_number}"] * keep.num_rows, type=pa.string()))
            if keep.num_rows:
                writer.write_table(keep)
        if measured.bytes_read != quarter.getinfo(name).file_size:
            raise ValueError(f"Source was not read in full: {name}")
        fingerprint = {
            "member": name, "sha256": measured.digest.hexdigest(), "bytes": measured.bytes_read,
            "row_count": count, "columns": columns,
            "dates": {date_column: {"min": datetime.strptime(low, "%Y%m").date().isoformat() if low else None,
                                    "max": datetime.strptime(high, "%Y%m").date().isoformat() if high else None}},
            "selected_rows": retained, "rows_removed": count - retained,
            "removal_reason": "loan_id_outside_fixed_hash_sample", "wall_seconds": time.perf_counter() - started,
        }
    emit("source_scan", year=year, quarter=quarter_number, **fingerprint)
    return loan_ids, fingerprint


def ingest_archives(repo, config):
    repo = Path(repo)
    if not config["years"] or not set(config["years"]).issubset(range(2005, 2012)):
        raise ValueError("Only the approved 2005 to 2011 archive years may be read")
    output = repo / config["output_dir"]
    reports = repo / config["report_dir"]
    output.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    writers = {}
    sources = []
    totals = {"selected_loans": 0, "selected_performance_rows": 0, "source_origination_rows": 0, "source_performance_rows": 0}
    with ExitStack() as stack:
        for kind, names in (("origination", ORIG_COLUMNS), ("performance", PERF_COLUMNS)):
            schema = pa.schema([(name, pa.string()) for name in names] + [("source_year", pa.int16()), ("source_quarter", pa.string())])
            writers[kind] = stack.enter_context(pq.ParquetWriter(output / f"raw_{kind}.parquet", schema, compression="zstd"))
        for year in config["years"]:
            path = repo / config["raw_dir"] / f"historical_data_{year}.zip"
            before = path.stat()
            source = {"path": path.relative_to(repo).as_posix(), "sha256": file_hash(path), "bytes": before.st_size, "members": []}
            with ZipFile(path) as annual:
                expected = {f"historical_data_{year}Q{number}.zip" for number in range(1, 5)}
                if set(annual.namelist()) != expected:
                    raise ValueError(f"Unexpected annual archive members: {path.name}")
                for number in range(1, 5):
                    quarter_name = f"historical_data_{year}Q{number}.zip"
                    with annual.open(quarter_name) as quarter_stream, ZipFile(quarter_stream) as quarter:
                        orig_name, perf_name = f"orig_{year}Q{number}.txt", f"perf_{year}Q{number}.txt"
                        if set(quarter.namelist()) != {orig_name, perf_name}:
                            raise ValueError(f"Unexpected quarterly archive members: {quarter_name}")
                        ids, orig = scan_member(quarter, orig_name, ORIG_COLUMNS, "first_payment_date", writers["origination"], year, number, config)
                        _, perf = scan_member(quarter, perf_name, PERF_COLUMNS, "period", writers["performance"], year, number, config, ids)
                        for item in (orig, perf):
                            item["quarter_archive"] = quarter_name
                            source["members"].append(item)
                        totals["selected_loans"] += orig["selected_rows"]
                        totals["selected_performance_rows"] += perf["selected_rows"]
                        totals["source_origination_rows"] += orig["row_count"]
                        totals["source_performance_rows"] += perf["row_count"]
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise ValueError(f"Archive changed during ingestion: {path.name}")
            sources.append(source)
    if not totals["selected_loans"] or not totals["selected_performance_rows"]:
        raise ValueError("The configured sample produced no usable input records")
    provenance = {"measured_at": datetime.now(timezone.utc).isoformat(), "ingest_code_sha256": file_hash(__file__),
                  "sampling": config["sample"], "schema_version": config["schema_version"], "sources": sources}
    (reports / "source_fingerprints.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    totals["parquet_sha256"] = {f"raw_{kind}.parquet": file_hash(output / f"raw_{kind}.parquet") for kind in writers}
    (reports / "ingestion_summary.json").write_text(json.dumps(totals, indent=2), encoding="utf-8")
    emit("ingestion_totals", **totals)
    return totals
