# Credit-Risk-PD-LGD-ECL-Engine

A staged mortgage credit-risk project using Freddie Mac Single-Family Loan-Level data and economic series from FRED.

The project will estimate the probability of default, loss given default, and exposure at default. These are the chance a borrower defaults, the share of the outstanding amount lost after default, and the amount owed when default occurs. They will feed an expected credit loss calculation and a regulatory capital calculation, with a separate validation pack.

## Current status

Repository setup only. Source data covering 2004 to 2012 is being downloaded by the project owner. File contents, origination coverage, and the end of the payment histories have not yet been checked. No loan data has been processed and no modelling results are available.

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

Stage-specific commands and dependency versions will be added as the stages are implemented and verified.

## Data and reproducibility

- Use actual Freddie Mac loan records. Download access requires the owner's registration.
- Keep source archives in `data/raw/`, or provide an existing local folder for inspection.
- Use FRED unemployment and house price series. Series selection and observation timing will be documented before use.
- Store processed data as Parquet and process it with DuckDB or Polars.
- Inspect data size before selecting the working sample. Any sample must be reproducible and documented.
- Split model development and evaluation by time. Do not randomly split monthly records belonging to the same loan.
- Record data definitions, exclusions, seeds, versions, and measured results as the work proceeds.

## Results and limitations

There are no results yet. The requested loan years include the financial crisis, but the available follow-up and loss fields still need inspection. The work is an educational modelling project; its IFRS 9 and Basel choices will need explicit assumptions and should not be presented as an approved bank model.

`README.md` is tracked. Stage explanations, the model development document, and interview preparation stay in the ignored `docs/` directory.
