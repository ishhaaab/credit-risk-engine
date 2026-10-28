# Credit-Risk-PD-LGD-ECL-Engine

A staged mortgage credit-risk project using Freddie Mac Single-Family Loan-Level data and economic series from FRED.

The project will estimate the probability of default, loss given default, and exposure at default. These are the chance a borrower defaults, the share of the outstanding amount lost after default, and the amount owed when default occurs. They will feed an expected credit loss calculation and a regulatory capital calculation, with a separate validation pack.

## Current status

Stage 1 loan processing is implemented and verified. The approved 2005 to 2011 archives contain 10,706,559 origination records and 708,602,901 monthly records. A deterministic 2% loan sample retains 214,212 loans and 14,181,422 monthly records before eligibility filters. Source histories extend through March 2026. No financial model has been fitted.

## Stages

1. Inspect the data, define default and early repayment, and build a monthly loan dataset with a record of every filter.
2. Model default and early repayment over time, then compare with a gradient boosting challenger.
3. Estimate losses after default using recoveries, costs, and the timing of cash flows. Calibrate a downturn estimate.
4. Project outstanding balances and arrears for mortgage exposure at default.
5. Calculate IFRS 9 expected credit losses under weighted economic scenarios.
6. Calculate Basel capital and compare estimates that follow current conditions with longer-run estimates.
7. Validate discrimination, calibration, stability, and performance on later loan vintages.
8. Prepare interview answers and resume bullets using measured results.

## Repository structure

- `src/`: data processing and modelling code.
- `tests/`: automated checks.
- `configs/`: reproducible run settings.
- `notebooks/`: exploration only.
- `docs/`: local explanations and personal project notes, excluded from Git.
- `data/`: local source and processed datasets, excluded from Git.

## Run Stage 1

Place the seven approved annual ZIPs in `data/raw/`. They must contain their four quarterly ZIPs and the `orig_YYYYQn.txt` and `perf_YYYYQn.txt` members. The observed field positions match Freddie Mac's July 2026 layout, with 31 origination and 35 performance fields.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python scripts/run_stage1.py --config configs/stage1.json
```

The last command reruns the loan-data stage. It streams the archives without extracting them, samples loan identifiers with SHA-256 and seed 1729, retains selected histories, and writes Parquet plus measured summaries. `--ingest-only` stops after sampling. `--panel-only` verifies the sampling configuration, source paths, layout, archive hashes, and cached Parquet hashes before rebuilding the derived panel.

Outputs stay under `data/processed/stage1/` and `outputs/stage1/`. `raw_origination.parquet` and `raw_performance.parquet` preserve source strings. `loans.parquet`, `loan_events.parquet`, and `loan_month_panel.parquet` hold cleaned contracts and histories. `pd_features.parquet` and `pd_targets.parquet` separate model inputs from outcomes. Join them by `loan_id` and `reporting_month`, never by row position. Only use derived outputs when the current run has produced `outputs/stage1/panel_success.json`.

### Event rules and holdouts

Default is the first observed month with 90 or more days delinquent, REO acquisition, or credit-exit code 02, 03, or 09. Voluntary payoff code 01 is prepayment only before legal maturity. Unknown status, observation gaps, transfers, and ambiguous modified-loan payoffs end usable PD observation. Post-default histories remain available for later loss analysis.

Training uses 2005 to 2007 vintages with target months through December 2011. Validation uses 2008 to 2009 vintages during 2012 to 2013. The out-of-time set uses 2010 to 2011 vintages during 2014 to 2016. Loans and target calendars do not overlap across splits. The later windows test surviving, seasoned loans rather than complete origination cohorts.

## Data and reproducibility

- Use actual Freddie Mac loan records. Download access requires the owner's registration.
- Keep source archives in `data/raw/`, or provide an existing local folder for inspection.
- National FRED candidates are BLS unemployment `UNRATE` and FHFA all-transactions house prices `USSTHPI`. Three ALFRED vintage CSVs have been retrieved as access checks. Monthly publication-aware features are not yet built or joined. The 2005 house-price vintage request returned HTTP 404 and needs resolution before macro modelling.
- Store processed data as Parquet and process it with DuckDB or Polars.
- Inspect data size before selecting the working sample. Any sample must be reproducible and documented.
- Split model development and evaluation by time. Do not randomly split monthly records belonging to the same loan.
- Record data definitions, exclusions, seeds, versions, and measured results as the work proceeds.

## Results and limitations

The verified panel contains 214,210 eligible loans and 14,181,212 monthly records. The PD dataset contains 5,596,514 monthly observations after observation and time-window filters. All eligible loans have performance histories.

| Split | Loans with labels | Monthly observations | First defaults | Prepayments |
| --- | ---: | ---: | ---: | ---: |
| Training | 80,842 | 3,672,819 | 8,364 | 43,198 |
| Validation | 42,219 | 731,007 | 737 | 20,432 |
| Out of time | 39,972 | 1,192,688 | 453 | 14,082 |

The source scan took 779.53 seconds and the final cached-input panel run took 519.97 seconds in this environment. These are separate measured commands, not a measured end-to-end duration. The first panel attempt exhausted DuckDB's temporary-disk allowance; the corrected implementation streams the wide output using narrow intermediate keys. The full test suite passes 28 tests.

The data is a retrospective extract, so corrected origination fields are not proven historical point-in-time snapshots. Event dates use the first observed reporting month, not a backdated zero-balance effective date. The sample contains 155 exit-date discrepancies, including 89 voluntary payoffs without an earlier or simultaneous arrears default. Their economic prepayment timing remains uncertain. Missing outcomes are censored, and this may be informative. The IFRS 9 and Basel calculations are future educational stages, not an approved bank model.

`README.md` is tracked. Stage explanations, the model development document, and interview preparation stay in the ignored `docs/` directory.

## Commit audit packets

Install the pinned audit dependencies with `python -m pip install -r requirements.txt`.
Run the checks with `python -m pytest -vv -ra --color=no`.

After each commit, generate mechanical evidence with:

```powershell
python scripts/make_audit_packet.py --manifest configs/audit_setup.json
```

Without `--narrative`, this creates a draft and exits with code 1. Write sections C and D in a local file under `docs/audit/`, then pass its path with `--narrative` to generate the combined packet. Packets and their JSON evidence sidecars stay in the ignored `docs/audit/` directory. Existing packets are not overwritten.

Use a stage-specific manifest to name every input and output dataset, random seed, and stage command. The generator inspects only declared datasets. It measures Parquet files or delimited files, including explicitly named nested ZIP members, without extracting archives. Declare `date_columns` and, for text dates, the `date_format`; headerless text also needs `columns` and `header: false`. Dataset roles are `input`, `output`, or `inout`.

The generator records the Git state, changes, environment, configuration contents, verbose pytest output, command durations, and declared dataset fingerprints. Pass `--include-run` for previously captured command records, `--red-flag` for an analyst's concern, and `--findings-response` for a response to an auditor. Exit code 1 means review the packet's red flags; exit code 2 means packet generation failed. Imported logs are explicitly labelled as historical evidence, not a fresh test run.

Use `configs/audit_stage1.json` for the Stage 1 packet. Its `recorded_zip` entries reuse the full member scans saved by ingestion, then verify the current annual-container hashes and complete member inventories. The packet labels the member measurements as recorded evidence. It does not claim to recount all archive rows during each audit. The local record's hash identifies its contents but does not authenticate the earlier execution.
