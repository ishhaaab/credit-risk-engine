"""Build observed histories and strictly separated monthly PD inputs/targets."""

from datetime import date
import json
from pathlib import Path

import duckdb

from .ingest import emit, file_hash
from .schema import MONEY_COLUMNS, STATIC_FEATURES


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def validate_splits(config):
    previous_end = None
    years = set()
    for name in ("train", "validation", "out_of_time"):
        split = config["splits"][name]
        start, end = date.fromisoformat(split["start"]), date.fromisoformat(split["end"])
        if start > end or (previous_end is not None and start <= previous_end) or years.intersection(split["vintages"]):
            raise ValueError("Split calendars or loan vintages overlap, or a window is reversed")
        if start.day != 1 or end.day != 1:
            raise ValueError("Split dates must be first-of-month dates")
        years.update(split["vintages"])
        previous_end = end
    if not set(config["years"]).issubset(years):
        raise ValueError("Every requested vintage must have a split")


def build_panel(repo, config):
    validate_splits(config)
    repo = Path(repo)
    output, reports = repo / config["output_dir"], repo / config["report_dir"]
    output.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    success = reports / "panel_success.json"
    if success.exists():
        success.unlink()
    filters = []
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit={literal(config['duckdb']['memory_limit'])}")
    connection.execute(f"SET threads={int(config['duckdb']['threads'])}")
    connection.execute(f"SET temp_directory={literal(output / '_duckdb_tmp')}")
    connection.execute("SET preserve_insertion_order=false")

    def number(sql):
        return connection.execute(sql).fetchone()[0]

    def records(sql):
        cursor = connection.execute(sql)
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def check(name, sql):
        violations = number(sql)
        emit("data_check", name=name, violations=violations)
        if violations:
            raise ValueError(f"{name}: {violations} violations")

    def audit_filter(reason, before, after, unit):
        record = {"reason": reason, "unit": unit, "rows_before": before, "rows_after": after, "rows_removed": before - after}
        filters.append(record)
        emit("filter", **record)

    try:
        connection.execute(f"CREATE VIEW raw_o AS SELECT * FROM read_parquet({literal(output / 'raw_origination.parquet')})")
        connection.execute(f"CREATE VIEW raw_p AS SELECT * FROM read_parquet({literal(output / 'raw_performance.parquet')})")
        check("sampled vintages outside configured years", f"SELECT count(*) FROM raw_o WHERE source_year NOT IN ({','.join(map(str, config['years']))})")
        check("duplicate sampled origination loan IDs", "SELECT count(*) FROM (SELECT loan_id FROM raw_o GROUP BY loan_id HAVING count(*) > 1)")
        check("invalid origination IDs or archive-year mismatch", "SELECT count(*) FROM raw_o WHERE NOT regexp_full_match(loan_id, 'F[0-9]{2}Q[1-4][0-9]{7}') OR try_cast(substr(loan_id,2,2) AS INTEGER)+2000 != source_year")
        connection.execute("CREATE TABLE duplicate_keys AS SELECT loan_id, period FROM raw_p GROUP BY loan_id, period HAVING count(*) > 1")
        before = number("SELECT count(*) FROM raw_p")
        if number("SELECT count(*) FROM duplicate_keys"):
            connection.execute("CREATE TABLE duplicate_rows AS SELECT DISTINCT p.* FROM raw_p p JOIN duplicate_keys d USING (loan_id, period)")
            check("conflicting duplicate loan-month records", "SELECT count(*) FROM (SELECT loan_id, period FROM duplicate_rows GROUP BY loan_id, period HAVING count(*) > 1)")
            connection.execute("CREATE VIEW unique_p AS SELECT p.* FROM raw_p p ANTI JOIN duplicate_keys d USING (loan_id, period) UNION ALL SELECT * FROM duplicate_rows")
        else:
            connection.execute("CREATE VIEW unique_p AS SELECT * FROM raw_p")
        audit_filter("exact_duplicate_performance_rows", before, number("SELECT count(*) FROM unique_p"), "loan_months")
        check("invalid monthly reporting dates", "SELECT count(*) FROM unique_p WHERE try_strptime(period, '%Y%m') IS NULL OR length(period) != 6")
        numeric_rules = {
            "fico": "try_cast(fico AS INTEGER) BETWEEN 300 AND 850",
            "vantage_score": "try_cast(vantage_score AS INTEGER) BETWEEN 300 AND 850",
            "original_dti": "try_cast(original_dti AS DOUBLE) BETWEEN 0 AND 65",
            "original_ltv": "try_cast(original_ltv AS DOUBLE) BETWEEN CASE WHEN harp_indicator='Y' THEN 1 ELSE 6 END AND CASE WHEN harp_indicator='Y' THEN 998 ELSE 105 END",
            "original_cltv": "try_cast(original_cltv AS DOUBLE) BETWEEN CASE WHEN harp_indicator='Y' THEN 1 ELSE 6 END AND CASE WHEN harp_indicator='Y' THEN 998 ELSE 200 END AND (try_cast(original_ltv AS DOUBLE)=999 OR try_cast(original_cltv AS DOUBLE)>=try_cast(original_ltv AS DOUBLE))",
            "mi_pct": "try_cast(mi_pct AS DOUBLE) BETWEEN 0 AND 55",
            "units": "try_cast(units AS INTEGER) BETWEEN 1 AND 4",
            "borrowers": "try_cast(borrowers AS INTEGER) IN (1,2)",
        }
        replacements = [f"CASE WHEN {rule} THEN try_cast({name} AS DOUBLE) END AS {name}" for name, rule in numeric_rules.items()]
        replacements += ["try_cast(original_upb AS DECIMAL(18,2)) AS original_upb", "try_cast(original_interest_rate AS DOUBLE) AS original_interest_rate", "try_cast(original_term AS INTEGER) AS original_term", "try_strptime(first_payment_date, '%Y%m')::DATE AS first_payment_date", "try_strptime(maturity_date, '%Y%m')::DATE AS maturity_date"]
        replaced = list(numeric_rules) + ["original_upb", "original_interest_rate", "original_term", "first_payment_date", "maturity_date"]
        split_case = "CASE " + " ".join(f"WHEN source_year IN ({','.join(map(str, value['vintages']))}) THEN {literal(name)}" for name, value in config["splits"].items()) + " END"
        connection.execute(f"CREATE TABLE loan_candidates AS SELECT * EXCLUDE ({','.join(replaced)}), {','.join(replacements)}, {split_case} AS split FROM raw_o")
        before = number("SELECT count(*) FROM loan_candidates")
        connection.execute("DELETE FROM loan_candidates WHERE amortization_type != 'FRM' OR interest_only != 'N'")
        after = number("SELECT count(*) FROM loan_candidates")
        audit_filter("not_fully_amortizing_fixed_rate", before, after, "loans")
        contract = "first_payment_date IS NOT NULL AND maturity_date IS NOT NULL AND original_upb > 0 AND original_interest_rate > 0 AND original_interest_rate < 100 AND original_term BETWEEN 1 AND 480 AND date_diff('month', first_payment_date, maturity_date)+1=original_term AND split IS NOT NULL"
        connection.execute(f"CREATE TABLE loans AS SELECT * FROM loan_candidates WHERE {contract}")
        audit_filter("invalid_contract_dates_balance_rate_or_term", after, number("SELECT count(*) FROM loans"), "loans")
        missing_histories = number("SELECT count(*) FROM loans ANTI JOIN unique_p USING (loan_id)")
        emit("eligible_loans_without_performance", loans=missing_histories,
             vintage_event_denominator="eligible_loans_with_observed_performance")
        missing = records("SELECT " + ",".join(f"count(*) FILTER (WHERE {name} IS NULL) AS {name}" for name in numeric_rules) + " FROM loans")[0]
        emit("missing_numeric_origination_fields", **missing)
        replacements = [f"try_cast(nullif({name},'') AS DECIMAL(18,2)) AS {name}" for name in MONEY_COLUMNS]
        replacements += ["try_strptime(period, '%Y%m')::DATE AS reporting_month", "try_cast(nullif(current_interest_rate,'') AS DOUBLE) AS current_interest_rate", "try_cast(nullif(remaining_legal_months,'') AS INTEGER) AS remaining_legal_months"]
        excluded = MONEY_COLUMNS + ["period", "current_interest_rate", "remaining_legal_months"]
        connection.execute(f"CREATE VIEW typed_p AS SELECT * EXCLUDE ({','.join(excluded)}), {','.join(replacements)} FROM unique_p")
        money_missing = records("SELECT " + ",".join(f"count(*) FILTER (WHERE {name}!='' AND try_cast({name} AS DECIMAL(18,2)) IS NULL) AS {name}" for name in MONEY_COLUMNS) + " FROM unique_p")[0]
        emit("nonempty_unparseable_money_tokens_retained_in_raw", **money_missing)
        # Sort only the observation keys. Sorting all origination and workout
        # fields, then storing successive wide copies, exhausts local spill disk.
        connection.execute("CREATE TABLE observation_order AS SELECT p.loan_id, reporting_month, lag(reporting_month) OVER (PARTITION BY p.loan_id ORDER BY reporting_month) AS previous_month FROM typed_p p SEMI JOIN loans o USING (loan_id)")
        connection.execute("CREATE VIEW joined AS SELECT p.* EXCLUDE(source_year,source_quarter), o.* EXCLUDE(loan_id), s.previous_month FROM typed_p p JOIN loans o USING (loan_id) JOIN observation_order s USING (loan_id,reporting_month)")
        joined_count = number("SELECT count(*) FROM joined")
        audit_filter("origination_eligibility_inner_join", number("SELECT count(*) FROM unique_p"), joined_count, "loan_months")
        check("duplicate cleaned loan-month keys", "SELECT count(*) FROM (SELECT loan_id, reporting_month FROM joined GROUP BY loan_id, reporting_month HAVING count(*)>1)")
        codes = ",".join(literal(value) for value in config["default"]["credit_exit_codes"])
        numeric_status = "regexp_full_match(delinquency_status, '[0-9]{1,2}')"
        default = f"({numeric_status} AND coalesce(try_cast(delinquency_status AS INTEGER) BETWEEN {int(config['default']['minimum_delinquency_months'])} AND 99, false)) OR delinquency_status={literal(config['default']['reo_status'])} OR zero_balance_code IN ({codes})"
        transfers = ",".join(literal(value) for value in config["transfer_codes"])
        payoff = literal(config["payoff_code"])
        connection.execute(f"""CREATE VIEW signals AS SELECT *, ({default}) AS default_signal,
            zero_balance_code={payoff} AS payoff_signal,
            coalesce(remaining_legal_months, CASE WHEN modification_flag NOT IN ('Y','P')
                THEN date_diff('month',reporting_month,maturity_date) END) AS remaining_for_payoff,
            zero_balance_code IN ({transfers}) AS transfer_signal,
            CASE WHEN previous_month IS NOT NULL AND date_diff('month',previous_month,reporting_month)>1
                THEN (previous_month+INTERVAL 1 MONTH)::DATE END AS gap_start
            FROM joined""")
        check("unrecognised zero balance codes", f"SELECT count(*) FROM signals WHERE zero_balance_code NOT IN ('',{payoff},{codes},{transfers})")
        connection.execute(f"""CREATE VIEW event_rows AS SELECT *,
            payoff_signal AND remaining_for_payoff>0 AND NOT default_signal AS prepayment_signal,
            payoff_signal AND remaining_for_payoff<=0 AND NOT default_signal AS maturity_signal,
            CASE WHEN default_signal THEN NULL
                 WHEN payoff_signal AND remaining_for_payoff IS NULL THEN 'payoff_unknown_maturity'
                 WHEN payoff_signal THEN NULL
                 WHEN transfer_signal THEN 'transfer_or_defect'
                 WHEN delinquency_status IN ('XX','') THEN 'unknown_status'
                 WHEN NOT {numeric_status} THEN 'invalid_status'
                 WHEN current_upb IS NULL OR current_upb<=0 THEN 'unexplained_balance'
            END AS local_censor_reason FROM signals""")
        connection.execute("""CREATE TABLE event_summary AS SELECT loan_id,
            min(reporting_month) AS first_observed_month, max(reporting_month) AS last_observed_month,
            min(reporting_month) FILTER (WHERE default_signal) AS first_default_month,
            min(reporting_month) FILTER (WHERE prepayment_signal) AS first_prepayment_month,
            min(reporting_month) FILTER (WHERE payoff_signal) AS payoff_month,
            min(reporting_month) FILTER (WHERE transfer_signal) AS transfer_month,
            min(gap_start) AS first_gap_month,
            min(reporting_month) FILTER (WHERE local_censor_reason IS NOT NULL) AS local_censor_month,
            arg_min(local_censor_reason,reporting_month) FILTER (WHERE local_censor_reason IS NOT NULL) AS local_censor_reason,
            min(reporting_month) FILTER (WHERE zero_balance_code!='') AS exit_month,
            arg_min(CASE WHEN transfer_signal THEN 'transfer_or_defect' WHEN maturity_signal THEN 'maturity'
                WHEN prepayment_signal THEN 'prepayment' WHEN default_signal THEN 'credit_liquidation'
                ELSE 'payoff_unspecified_maturity' END, reporting_month) FILTER (WHERE zero_balance_code!='') AS exit_reason
            FROM event_rows GROUP BY loan_id""")
        connection.execute("""CREATE TABLE loan_events AS SELECT e.*, l.source_year AS vintage_year, l.split,
            coalesce(first_default_month=first_observed_month,false) AS default_at_entry,
            least(first_gap_month,local_censor_month) AS pd_censor_month,
            CASE WHEN first_gap_month IS NOT NULL AND (local_censor_month IS NULL OR first_gap_month<=local_censor_month)
                 THEN 'gap' ELSE local_censor_reason END AS pd_censor_reason
            FROM event_summary e JOIN loans l USING (loan_id)""")
        connection.execute("""CREATE VIEW loan_month_panel AS SELECT e.*, s.first_observed_month,
            s.first_default_month,s.first_prepayment_month,s.payoff_month,s.pd_censor_month,s.default_at_entry,
            coalesce(e.reporting_month=s.first_default_month,false) AS default_event,
            coalesce(e.reporting_month=s.first_prepayment_month AND (s.first_default_month IS NULL OR s.first_prepayment_month<s.first_default_month),false) AS prepayment_event,
            (s.first_default_month IS NULL OR e.reporting_month<=s.first_default_month)
            AND (s.payoff_month IS NULL OR e.reporting_month<=s.payoff_month)
            AND (s.pd_censor_month IS NULL OR e.reporting_month<s.pd_censor_month) AS at_risk
            FROM event_rows e JOIN loan_events s USING (loan_id)""")
        connection.execute("CREATE VIEW risk AS SELECT * FROM loan_month_panel WHERE at_risk")
        count = number("SELECT count(*) FROM risk")
        audit_filter("after_first_default_payoff_or_observation_censor", joined_count, count, "loan_months")
        connection.execute("CREATE VIEW eligible_risk AS SELECT * FROM risk WHERE previous_month IS NOT NULL AND reporting_month>=first_payment_date AND NOT default_at_entry")
        next_count = number("SELECT count(*) FROM eligible_risk")
        audit_filter("no_prior_observation_pre_first_payment_or_prevalent_default", count, next_count, "loan_months")
        windows = " OR ".join(f"(split={literal(name)} AND reporting_month BETWEEN DATE {literal(value['start'])} AND DATE {literal(value['end'])})" for name, value in config["splits"].items())
        connection.execute(f"CREATE VIEW model_rows AS SELECT * FROM eligible_risk WHERE {windows}")
        audit_filter("outside_assigned_time_window", next_count, number("SELECT count(*) FROM model_rows"), "loan_months")
        check("simultaneous competing events", "SELECT count(*) FROM model_rows WHERE default_event AND prepayment_event")
        check("future or nonconsecutive feature availability", "SELECT count(*) FROM model_rows WHERE previous_month>=reporting_month OR date_diff('month',previous_month,reporting_month)!=1")
        check("repeated first events", "SELECT count(*) FROM (SELECT loan_id FROM model_rows GROUP BY loan_id HAVING sum(default_event::INTEGER)>1 OR sum(prepayment_event::INTEGER)>1)")
        connection.execute(f"CREATE VIEW pd_features AS SELECT loan_id,reporting_month,split,previous_month AS feature_asof_month,date_diff('month',first_payment_date,reporting_month)+1 AS loan_age_months,{','.join(STATIC_FEATURES)} FROM model_rows")
        connection.execute("CREATE VIEW pd_targets AS SELECT loan_id,reporting_month,split,default_event,prepayment_event FROM model_rows")
        split_metrics = records("SELECT split,count(*) AS loan_months,count(DISTINCT loan_id) AS loans,sum(default_event::INTEGER) AS defaults,sum(prepayment_event::INTEGER) AS prepayments,min(reporting_month) AS first_target_month,max(reporting_month) AS last_target_month FROM pd_targets GROUP BY split ORDER BY first_target_month")
        vintage_metrics = records("SELECT vintage_year, 'descriptive_full_followup_not_model_fit' AS split,count(*) AS loans,count(first_default_month) AS observed_default_loans,count(*) FILTER (WHERE first_prepayment_month IS NOT NULL AND (first_default_month IS NULL OR first_prepayment_month<first_default_month)) AS prepaid_before_observed_default,sum(default_at_entry::INTEGER) AS defaults_at_entry,count(pd_censor_month) AS loans_with_observation_censor FROM loan_events GROUP BY vintage_year ORDER BY vintage_year")
        for row in split_metrics:
            emit("split_event_counts", **row)
        for row in vintage_metrics:
            emit("vintage_observed_events", **row)
        paths = []
        for table in ("loans", "loan_events", "loan_month_panel", "pd_features", "pd_targets"):
            path = output / f"{table}.parquet"
            connection.execute(f"COPY {table} TO {literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384)")
            paths.append(path)
        summary = {"filters": filters, "split_metrics": split_metrics, "vintage_metrics": vintage_metrics,
                   "eligible_loans_without_performance": missing_histories,
                   "configuration": config,
                   "origination_missing_counts": missing, "nonempty_unparseable_money_counts": money_missing,
                   "clipped_values": 0, "winsorized_values": 0, "macro_variables_joined": [],
                   "panel_code_sha256": file_hash(__file__), "parquet_sha256": {path.name: file_hash(path) for path in paths}}
        (reports / "panel_summary.json").write_text(json.dumps(summary, indent=2, default=str, allow_nan=False), encoding="utf-8")
        success.write_text(json.dumps({"summary_sha256": file_hash(reports / "panel_summary.json"), "outputs": summary["parquet_sha256"]}, indent=2), encoding="utf-8")
        emit("panel_finished", loan_months=joined_count, model_rows=number("SELECT count(*) FROM model_rows"), clipped_values=0, winsorized_values=0, macro_variables_joined=[])
        return summary
    finally:
        connection.close()
