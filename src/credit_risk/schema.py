"""Field order verified against Freddie Mac Release 47, July 2026.

Source URLs and SHA-256 identifiers are recorded in configs/stage1.json.
Raw fields stay strings so missing codes and recovery signs survive ingestion.
"""

ORIG_COLUMNS = [
    "fico", "first_payment_date", "first_time_homebuyer", "maturity_date", "msa",
    "mi_pct", "units", "occupancy", "original_cltv", "original_dti", "original_upb",
    "original_ltv", "original_interest_rate", "channel", "prepayment_penalty",
    "amortization_type", "state", "property_type", "postal_code", "loan_id",
    "loan_purpose", "original_term", "borrowers", "seller_name", "super_conforming",
    "pre_harp_loan_id", "special_program", "harp_indicator", "valuation_method",
    "interest_only", "vantage_score",
]

PERF_COLUMNS = [
    "loan_id", "period", "current_upb", "delinquency_status", "reported_loan_age",
    "remaining_legal_months", "defect_settlement_date", "modification_flag",
    "zero_balance_code", "zero_balance_date", "current_interest_rate", "deferred_upb",
    "ddlpi", "mi_recoveries", "net_sales_proceeds", "non_mi_recoveries",
    "total_expenses", "legal_costs", "maintenance_costs", "taxes_insurance",
    "misc_expenses", "actual_loss", "cumulative_modification_costs",
    "step_modification", "payment_deferral", "estimated_ltv", "removal_upb",
    "delinquent_interest", "disaster_flag", "borrower_assistance",
    "current_modification_costs", "interest_bearing_upb", "mi_cancellation",
    "servicer_name", "bankruptcy_cramdown_costs",
]

MONEY_COLUMNS = [
    "current_upb", "deferred_upb", "mi_recoveries", "net_sales_proceeds",
    "non_mi_recoveries", "total_expenses", "legal_costs", "maintenance_costs",
    "taxes_insurance", "misc_expenses", "actual_loss", "cumulative_modification_costs",
    "removal_upb", "delinquent_interest", "current_modification_costs",
    "interest_bearing_upb", "bankruptcy_cramdown_costs",
]

# Target-month servicing values and all workout fields are deliberately absent.
STATIC_FEATURES = [
    "fico", "original_cltv", "original_dti", "original_ltv", "original_upb",
    "original_interest_rate", "original_term", "mi_pct", "units", "borrowers",
    "occupancy", "loan_purpose", "state", "property_type", "channel",
    "first_time_homebuyer", "harp_indicator",
]
