"""Small invented histories test rules. None are reported as mortgage results."""

import copy
from datetime import date
import importlib
import io
import json
from pathlib import Path
import sys
from zipfile import ZipFile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def module(name):
    assert (ROOT / "src/credit_risk" / f"{name}.py").is_file(), "Stage 1 is not implemented yet"
    return importlib.import_module(f"credit_risk.{name}")


def configuration():
    return json.loads((ROOT / "configs/stage1.json").read_text())


def origination(loan, first="200501", maturity="203412", term="360"):
    # Positions come from the official 31-field layout, not the implementation.
    row = ["700", first, "N", maturity, "", "0", "1", "P", "80", "35",
           "100000", "80", "6.000", "R", "N", "FRM", "CA", "SF", "900",
           loan, "P", term, "1", "OTHER", "N", "", "", "N", "7", "N", "9999"]
    return row


def performance(loan, period, delinquency="00", code="", balance="100000", remaining="300"):
    row = [""] * 35
    row[0:6] = [loan, period, balance, delinquency, "1", remaining]
    row[8:12] = [code, period if code else "", "6.000", "0.00"]
    row[25] = "999"
    row[31:35] = [balance, "7", "OTHER", ""]
    return row


def write_raw(repo, originations, performances):
    schema = module("schema")
    output = repo / "data/processed/stage1"
    output.mkdir(parents=True, exist_ok=True)
    for filename, names, rows in (("raw_origination.parquet", schema.ORIG_COLUMNS, originations),
                                  ("raw_performance.parquet", schema.PERF_COLUMNS, performances)):
        table = pa.table({name: pa.array([row[i] for row in rows], type=pa.string()) for i, name in enumerate(names)})
        id_position = 19 if filename.startswith("raw_orig") else 0
        table = table.append_column("source_year", pa.array([2000 + int(row[id_position][1:3]) for row in rows], type=pa.int16()))
        table = table.append_column("source_quarter", pa.array([row[id_position][1:5] for row in rows]))
        pq.write_table(table, output / filename)
    return output


def read_output(repo, name):
    return pq.read_table(repo / "data/processed/stage1" / f"{name}.parquet").to_pylist()


def test_hash_sample_is_fixed_and_independent_of_row_order_and_batches():
    select = module("ingest").select_loan_ids
    ids = [f"F05Q1{i:07d}" for i in range(1, 11)]
    expected = {"F05Q10000001", "F05Q10000003", "F05Q10000005", "F05Q10000006",
                "F05Q10000007", "F05Q10000009", "F05Q10000010"}
    assert select(ids, 1729, 1, 2) == expected
    assert select(list(reversed(ids)), 1729, 1, 2) == expected
    assert select(ids[:4], 1729, 1, 2) | select(ids[4:], 1729, 1, 2) == expected
    assert select(ids, 1729, 0, 100) == set()
    assert select(ids, 1729, 100, 100) == set(ids)
    with pytest.raises(ValueError):
        select(ids, 1729, 101, 100)


def test_nested_ingestion_keeps_whole_histories_and_measures_all_source_rows(tmp_path):
    ingest = module("ingest")
    raw = tmp_path / "data/raw"
    raw.mkdir(parents=True)
    loan = "F05Q10000001"
    config = configuration()
    config["years"] = [2005]
    config["sample"] = {"seed": 1729, "numerator": 100, "denominator": 100}
    with ZipFile(raw / "historical_data_2005.zip", "w") as annual:
        for quarter in range(1, 5):
            loan = f"F05Q{quarter}0000001"
            buffer = io.BytesIO()
            with ZipFile(buffer, "w") as zipped:
                zipped.writestr(f"orig_2005Q{quarter}.txt", "|".join(origination(loan)) + "\n")
                zipped.writestr(f"perf_2005Q{quarter}.txt", "\n".join("|".join(row) for row in [
                    performance(loan, "200501"), performance(loan, "200502", "03"),
                    performance(loan, "200503", "00")]) + "\n")
            annual.writestr(f"historical_data_2005Q{quarter}.zip", buffer.getvalue())
    result = ingest.ingest_archives(tmp_path, config)
    assert result["selected_loans"] == 4
    assert result["selected_performance_rows"] == 12
    fingerprints = json.loads((tmp_path / "outputs/stage1/source_fingerprints.json").read_text())
    assert len(fingerprints["sources"]) == 1
    assert len(fingerprints["sources"][0]["members"]) == 8
    assert sum(item["row_count"] for item in fingerprints["sources"][0]["members"]) == 16
    assert len(read_output(tmp_path, "raw_performance")) == 12
    assert list(raw.iterdir()) == [raw / "historical_data_2005.zip"]
    assert hasattr(ingest, "validate_cached_ingestion"), "Cached runs must validate their ingestion configuration"
    ingest.validate_cached_ingestion(tmp_path, config)
    for key, value in (("years", [2006]), ("schema_version", "different-layout"),
                       ("raw_dir", "different-source"),
                       ("sample", {"seed": 99, "numerator": 100, "denominator": 100})):
        changed = copy.deepcopy(config)
        changed[key] = value
        with pytest.raises(ValueError, match="configuration"):
            ingest.validate_cached_ingestion(tmp_path, changed)
    with ZipFile(raw / "historical_data_2005.zip", "a") as annual:
        annual.writestr("extra.txt", "changed source")
    with pytest.raises(ValueError, match="Archive changed"):
        ingest.validate_cached_ingestion(tmp_path, config)


def test_panel_rejects_vintages_outside_its_requested_input_scope(tmp_path):
    panel = module("panel")
    loan = "F10Q10000001"
    write_raw(tmp_path, [origination(loan, first="201001", maturity="203912")],
              [performance(loan, "201312"), performance(loan, "201401")])
    config = configuration()
    config["years"] = [2005]
    with pytest.raises(ValueError, match="configured years"):
        panel.build_panel(tmp_path, config)


def test_eligible_origination_without_history_is_counted(tmp_path):
    panel = module("panel")
    loans = ["F05Q10000001", "F05Q10000002"]
    write_raw(tmp_path, [origination(loan) for loan in loans],
              [performance(loans[0], "200501"), performance(loans[0], "200502")])
    summary = panel.build_panel(tmp_path, configuration())
    assert summary.get("eligible_loans_without_performance") == 1
    assert summary["vintage_metrics"][0]["loans"] == 1


def test_default_prepayment_maturity_censoring_and_workout_retention(tmp_path):
    panel = module("panel")
    loans = [f"F05Q1{i:07d}" for i in range(1, 9)]
    originations = [origination(loan) for loan in loans]
    originations[2] = origination(loans[2], maturity="200503", term="3")
    rows = [performance(loan, "200501") for loan in loans]
    rows.extend([
        performance(loans[0], "200502", "02"), performance(loans[0], "200503", "03"),
        performance(loans[0], "200504", "00"), performance(loans[0], "200505", "03"),
        performance(loans[1], "200502", code="01", balance="0"),
        performance(loans[2], "200502", remaining="1"),
        performance(loans[2], "200503", code="01", balance="0", remaining="0"),
        performance(loans[3], "200502", "RA"),
        performance(loans[4], "200502", code="96", balance="0"),
        performance(loans[5], "200503"), performance(loans[5], "200504", "03"),
        performance(loans[6], "200502", "XX"), performance(loans[6], "200503", "03"),
    ])
    rows[7][3] = "03"  # Already defaulted when observation starts.
    rows.append(performance(loans[7], "200502", "00"))
    write_raw(tmp_path, originations, rows)
    panel.build_panel(tmp_path, configuration())
    targets = read_output(tmp_path, "pd_targets")
    assert sum(row["default_event"] for row in targets) == 2
    assert sum(row["prepayment_event"] for row in targets) == 1
    assert {row["loan_id"] for row in targets} == set(loans[:4])
    assert not any(row["loan_id"] == loans[0] and row["reporting_month"] > date(2005, 3, 1) for row in targets)
    assert len(read_output(tmp_path, "loan_month_panel")) == len(rows)
    events = {row["loan_id"]: row for row in read_output(tmp_path, "loan_events")}
    assert events[loans[2]]["exit_reason"] == "maturity"
    assert events[loans[4]]["exit_reason"] == "transfer_or_defect"
    assert events[loans[5]]["pd_censor_reason"] == "gap"
    assert events[loans[6]]["pd_censor_reason"] == "unknown_status"
    assert events[loans[7]]["default_at_entry"] is True


def test_credit_exit_is_default_even_when_delinquency_is_unavailable(tmp_path):
    panel = module("panel")
    loans = [f"F05Q1{i:07d}" for i in range(1, 4)]
    rows = [performance(loan, "200501") for loan in loans]
    rows += [performance(loan, "200502", "XX", code=code, balance="0")
             for loan, code in zip(loans, ["02", "03", "09"])]
    write_raw(tmp_path, [origination(loan) for loan in loans], rows)
    panel.build_panel(tmp_path, configuration())
    assert sum(row["default_event"] for row in read_output(tmp_path, "pd_targets")) == 3


def test_features_exclude_outcomes_and_future_changes_do_not_alter_training_rows(tmp_path):
    panel = module("panel")
    loan = "F07Q10000001"
    originations = [origination(loan, first="200701", maturity="203612")]
    rows = [performance(loan, "201111"), performance(loan, "201112", "01"), performance(loan, "201201", "03")]
    write_raw(tmp_path, originations, rows)
    panel.build_panel(tmp_path, configuration())
    before = read_output(tmp_path, "pd_features")
    assert len(before) == 1
    assert before[0]["reporting_month"] == date(2011, 12, 1)
    forbidden = {"delinquency_status", "default_event", "prepayment_event", "actual_loss", "zero_balance_code", "current_upb", "first_default_month"}
    assert not forbidden.intersection(before[0])
    assert before[0]["feature_asof_month"] < before[0]["reporting_month"]
    rows[-1][3] = "00"
    rows[-1][21] = "999999.99"
    write_raw(tmp_path, originations, rows)
    panel.build_panel(tmp_path, configuration())
    assert read_output(tmp_path, "pd_features") == before


def test_splits_are_disjoint_by_loan_and_by_target_calendar(tmp_path):
    panel = module("panel")
    cases = [("F07Q10000001", "200701", "203612", "201111", "201112"),
             ("F08Q10000001", "200801", "203712", "201112", "201201"),
             ("F10Q10000001", "201001", "203912", "201312", "201401")]
    orig = [origination(loan, first, maturity) for loan, first, maturity, _, _ in cases]
    perf = [performance(loan, month) for loan, _, _, before, after in cases for month in [before, after]]
    write_raw(tmp_path, orig, perf)
    panel.build_panel(tmp_path, configuration())
    targets = read_output(tmp_path, "pd_targets")
    assert [row["split"] for row in sorted(targets, key=lambda row: row["reporting_month"])] == ["train", "validation", "out_of_time"]
    assert len({row["loan_id"] for row in targets}) == 3


def test_exact_duplicates_are_counted_and_conflicting_loan_months_fail(tmp_path):
    panel = module("panel")
    loan = "F05Q10000001"
    row = performance(loan, "200501")
    rows = [row, copy.deepcopy(row), performance(loan, "200502")]
    write_raw(tmp_path, [origination(loan)], rows)
    summary = panel.build_panel(tmp_path, configuration())
    duplicate_filter = next(item for item in summary["filters"] if item["reason"] == "exact_duplicate_performance_rows")
    assert duplicate_filter["rows_removed"] == 1
    assert len(read_output(tmp_path, "loan_month_panel")) == 2
    rows[1][3] = "03"
    write_raw(tmp_path, [origination(loan)], rows)
    with pytest.raises(ValueError, match="conflicting.*loan.month"):
        panel.build_panel(tmp_path, configuration())


def test_missing_sentinels_are_null_not_excluded_or_treated_as_real_scores(tmp_path):
    panel = module("panel")
    loan = "F05Q10000001"
    row = origination(loan)
    row[0], row[8], row[9], row[11] = "9999", "999", "999", "999"
    write_raw(tmp_path, [row], [performance(loan, "200501"), performance(loan, "200502")])
    panel.build_panel(tmp_path, configuration())
    feature = read_output(tmp_path, "pd_features")[0]
    assert feature["fico"] is None
    assert feature["original_cltv"] is None
    assert feature["original_dti"] is None
    assert feature["original_ltv"] is None


def test_overlapping_split_windows_are_rejected_before_processing(tmp_path):
    panel = module("panel")
    config = configuration()
    config["splits"]["validation"]["start"] = "2011-01-01"
    with pytest.raises(ValueError, match="overlap"):
        panel.build_panel(tmp_path, config)


@pytest.mark.parametrize("status", ["-1", "100", "2.5"])
def test_invalid_numeric_status_censors_instead_of_becoming_a_survivor(tmp_path, status):
    panel = module("panel")
    loan = "F05Q10000001"
    write_raw(tmp_path, [origination(loan)], [performance(loan, "200501"),
        performance(loan, "200502", status), performance(loan, "200503", "03")])
    panel.build_panel(tmp_path, configuration())
    assert read_output(tmp_path, "pd_targets") == []
    assert read_output(tmp_path, "loan_events")[0]["pd_censor_reason"] == "invalid_status"


def test_modified_payoff_without_maturity_is_not_labelled_as_survival(tmp_path):
    panel = module("panel")
    loan = "F05Q10000001"
    payoff = performance(loan, "200502", code="01", balance="0", remaining="")
    payoff[7] = "P"
    write_raw(tmp_path, [origination(loan)], [performance(loan, "200501"), payoff])
    panel.build_panel(tmp_path, configuration())
    assert read_output(tmp_path, "pd_targets") == []
    event = read_output(tmp_path, "loan_events")[0]
    assert event["pd_censor_reason"] == "payoff_unknown_maturity"
    assert event["exit_reason"] == "payoff_unspecified_maturity"


def test_panel_build_fits_bounded_memory_and_spill_budget(tmp_path, monkeypatch):
    panel = module("panel")
    loans = [f"F05Q1{i:07d}" for i in range(1, 1001)]
    months = [f"{year}{month:02d}" for year in range(2005, 2010) for month in range(1, 13)]
    write_raw(tmp_path, [origination(loan) for loan in loans],
              [performance(loan, month) for loan in loans for month in months])
    config = configuration()
    # Leave room for the wide Parquet writer's fixed per-column buffers.
    config["duckdb"] = {"memory_limit": "64MB", "threads": 1}
    connect = duckdb.connect

    def limited_connection():
        connection = connect()
        connection.execute("SET max_temp_directory_size='32MB'")
        return connection

    monkeypatch.setattr(panel.duckdb, "connect", limited_connection)
    panel.build_panel(tmp_path, config)
    assert pq.ParquetFile(tmp_path / "data/processed/stage1/loan_month_panel.parquet").metadata.num_rows == 60000
    assert pq.ParquetFile(tmp_path / "data/processed/stage1/pd_targets.parquet").metadata.num_rows == 59000
