# SaaS Metrics Pipeline

An end-to-end analytics engineering pipeline that models subscription lifecycle data for a fictional B2B SaaS company and produces core financial metrics: MRR, churn, cohort retention, and customer lifetime value.

Built to demonstrate dimensional modeling, dbt-style transformation layers, data quality testing, and analytical storytelling — the core skills of an Analytics Engineer or Technical Data Analyst role.

## Dashboard Output

![SaaS Metrics Dashboard](output/saas_metrics_dashboard.png)

## Architecture

```
Raw CSV Data                Staging Layer              Intermediate Layer           Mart Layer
─────────────             ────────────────           ──────────────────          ──────────────
raw_customers  ──────►   stg_customers    ──────►   int_subscription_months ──► fct_mrr_summary
raw_plans      ──────►   stg_plans        ──────►   int_mrr_movements      ──► fct_cohort_retention
raw_subscriptions ───►   stg_subscriptions                                  ──► dim_customer_ltv
raw_events     ──────►   stg_events
```

The pipeline runner loads CSVs into SQLite and executes each SQL model in dependency order — mimicking `dbt run` without requiring dbt installation. All models are plain `.sql` files that can be lifted directly into a dbt project.

## Key Metrics

| Metric | Definition | Why It Matters |
|--------|-----------|----------------|
| MRR (Monthly Recurring Revenue) | Sum of all active subscription revenue in a given month | The headline SaaS financial metric |
| Net MRR Change | New + Expansion + Reactivation − Churn − Contraction | Shows whether the business is growing or shrinking |
| Gross Customer Churn Rate | Churned customers / start-of-month active customers | Measures product-market fit |
| Gross Revenue Churn Rate | Lost MRR / start-of-month MRR | Revenue-weighted view of retention |
| ARPU | Total MRR / Active Customers | Pricing power and mix shift indicator |
| Cohort Retention | % of a signup cohort still active N months later | The best measure of long-term product health |
| Customer LTV | Historical revenue + projected future revenue | Customer-level profitability estimate |

## Project Structure

```
saas-metrics-pipeline/
├── README.md
├── requirements.txt
├── run_pipeline.py              # Pipeline orchestrator
├── data/
│   ├── generate_data.py         # Synthetic data generator
│   ├── raw_customers.csv
│   ├── raw_plans.csv
│   ├── raw_subscriptions.csv
│   └── raw_events.csv
├── models/
│   ├── staging/                 # Clean + type-cast raw sources
│   │   ├── stg_customers.sql
│   │   ├── stg_plans.sql
│   │   ├── stg_subscriptions.sql
│   │   └── stg_events.sql
│   ├── intermediate/            # Business logic transforms
│   │   ├── int_subscription_months.sql   # Monthly subscription spine
│   │   └── int_mrr_movements.sql         # MRR change classification
│   └── marts/                   # Business-facing tables
│       ├── fct_mrr_summary.sql           # Monthly MRR waterfall
│       ├── fct_cohort_retention.sql      # Cohort retention rates
│       └── dim_customer_ltv.sql          # Customer lifetime value
├── analysis/
│   └── analyze.py               # Visualization + analytical summary
└── output/
    ├── fct_mrr_summary.csv
    ├── fct_cohort_retention.csv
    ├── dim_customer_ltv.csv
    └── saas_metrics_dashboard.png
```

## Quick Start

```bash
git clone https://github.com/Kablan-ASBN/saas_metrics_pipeline.git
cd saas_metrics_pipeline
pip install -r requirements.txt

# Generate synthetic data
python data/generate_data.py

# Run the pipeline (staging → intermediate → marts + quality checks)
python run_pipeline.py

# Generate analysis dashboard
python analysis/analyze.py
```

## Data Quality Checks

The pipeline includes 5 built-in assertions that run after every execution:

| Check | What It Validates |
|-------|-------------------|
| No negative MRR | Subscription months never have negative revenue |
| All customers in LTV | Every customer appears in the final dimension table |
| Retention ≤ 100% | Cohort retention rates are mathematically valid |
| Full month coverage | MRR summary spans all 36 months of the analysis period |
| No orphaned events | Every event references a valid subscription |

## Design Decisions

**Why synthetic data?** Using generated data means anyone can clone this repo and run it end-to-end without API keys, downloads, or data agreements. The generator models realistic patterns (seasonal signups, tenure-dependent churn curves, plan upgrade paths) so the pipeline handles real-world complexity.

**Why SQLite instead of Postgres/DuckDB?** Zero external dependencies. The SQL models use standard SQL and can be migrated to any warehouse with minimal changes. The `.sql` files are also directly compatible with dbt if someone wants to extend this project.

**Why not dbt directly?** This project demonstrates understanding of what dbt does under the hood — dependency ordering, table materialization, testing — without hiding behind the tool. In an interview, I can explain every step of the DAG because I built the orchestration layer.

## Known Limitations

- **LTV projection is simplistic.** The 6-month forward projection uses flat average MRR rather than a survival-curve-weighted estimate. A production model would use Kaplan-Meier or a probabilistic approach.
- **MRR carry-forward logic uses a correlated subquery.** The `int_subscription_months` model handles months with no events via a correlated subquery, which would not scale well beyond ~100K rows. In a warehouse context, this would use `LAST_VALUE` with `IGNORE NULLS`.
- **No incremental loading.** The pipeline does a full rebuild on every run. A production pipeline would use incremental materialization for the subscription spine.
- **Customer churn rate denominator is approximate.** It uses active customers at end of month + churned, rather than a true beginning-of-month count from the prior month.

## Analytical Findings

From the generated dataset for Acme Analytics (2022–2024):

- **MRR grew from $3.2K to $93.9K** over 36 months, driven primarily by new customer acquisition with steady expansion revenue from upgrades.
- **ARPU increased from ~$148 to ~$199**, indicating successful upselling into higher-tier plans over time.
- **Average monthly customer churn is ~4%**, with a visible downward trend in later months as the customer base matures and early-tenure churn stabilizes.
- **Cohort retention curves flatten around 55-70%** after month 10, suggesting strong long-term retention once customers survive the first few months.

## Technologies

- **Python** — data generation, pipeline orchestration, analysis
- **SQL (SQLite)** — transformation models following dbt conventions
- **pandas** — data loading and export
- **matplotlib / seaborn** — analytical visualizations