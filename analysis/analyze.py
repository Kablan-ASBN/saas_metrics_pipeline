"""
analyze.py
Reads from the pipeline's SQLite database and produces a 6-panel dashboard
of the key SaaS metrics. This is the "so what?" layer — it turns the
mart tables into visual insights that tell a story.

The 6 charts:
1. MRR Growth         — the headline number, is revenue going up?
2. MRR Waterfall      — what's driving that growth (or decline)?
3. Cohort Retention   — are customers sticking around?
4. Customer Health    — how does our customer base break down by risk?
5. ARPU Trend         — are we moving upmarket or downmarket?
6. Churn Rate         — is our leaky bucket getting better or worse?
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore")

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "saas_metrics.db")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Chart styling ---
# Using seaborn's whitegrid for clean, professional-looking charts.
# The color palette is chosen to be colorblind-friendly-ish and
# to clearly separate positive (green/blue) from negative (red/yellow) signals.
sns.set_theme(style="whitegrid", font_scale=1.1)

COLORS = {
    "primary": "#2563EB",       # blue — main metric line
    "positive": "#10B981",      # green — good things
    "negative": "#EF4444",      # red — bad things
    "warning": "#F59E0B",       # yellow/amber — caution
    "neutral": "#6B7280",       # gray — labels
    "expansion": "#8B5CF6",     # purple — expansion revenue
    "new": "#2563EB",           # blue — new revenue
    "churn": "#EF4444",         # red — lost revenue
    "contraction": "#F59E0B",   # yellow — contraction
    "reactivation": "#10B981",  # green — win-backs
}


def load_data():
    """Pull all three mart tables from the database."""
    conn = sqlite3.connect(DB_PATH)
    mrr = pd.read_sql("SELECT * FROM fct_mrr_summary ORDER BY month_id", conn)
    cohort = pd.read_sql("SELECT * FROM fct_cohort_retention", conn)
    ltv = pd.read_sql("SELECT * FROM dim_customer_ltv", conn)
    conn.close()
    return mrr, cohort, ltv



# Chart 1: MRR Growth
# The single most important SaaS metric. If this line is going up
# and to the right, the business is healthy.

def plot_mrr_growth(mrr, ax):
    x = range(len(mrr))
    ax.fill_between(x, mrr["total_mrr"], alpha=0.15, color=COLORS["primary"])
    ax.plot(x, mrr["total_mrr"], color=COLORS["primary"], linewidth=2.5)

    # Label the start and end points so the growth is immediately obvious
    ax.annotate(
        f"${mrr['total_mrr'].iloc[0]:,.0f}",
        xy=(0, mrr["total_mrr"].iloc[0]),
        fontsize=9, color=COLORS["neutral"], ha="left", va="bottom"
    )
    ax.annotate(
        f"${mrr['total_mrr'].iloc[-1]:,.0f}",
        xy=(len(mrr) - 1, mrr["total_mrr"].iloc[-1]),
        fontsize=11, fontweight="bold", color=COLORS["primary"],
        ha="right", va="bottom"
    )

    ax.set_title("Monthly Recurring Revenue (MRR)", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("MRR ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xticks(range(0, len(mrr), 6))
    ax.set_xticklabels(mrr["month_id"].iloc[::6], rotation=45, ha="right")
    ax.set_xlim(0, len(mrr) - 1)



# Chart 2: MRR Waterfall
# Breaks down each month's MRR change into its components.
# Blue bars above zero = new revenue. Red bars below zero = lost revenue.
# If the blue consistently outweighs the red, you're growing.

def plot_mrr_waterfall(mrr, ax):
    x = range(len(mrr))

    # Stack positive components above zero
    ax.bar(x, mrr["new_mrr"], label="New", color=COLORS["new"], alpha=0.85, width=0.8)
    ax.bar(x, mrr["expansion_mrr"], bottom=mrr["new_mrr"],
           label="Expansion", color=COLORS["expansion"], alpha=0.85, width=0.8)
    ax.bar(x, mrr["reactivation_mrr"],
           bottom=mrr["new_mrr"] + mrr["expansion_mrr"],
           label="Reactivation", color=COLORS["reactivation"], alpha=0.85, width=0.8)

    # Stack negative components below zero
    ax.bar(x, mrr["churn_mrr"], label="Churn", color=COLORS["churn"], alpha=0.85, width=0.8)
    ax.bar(x, mrr["contraction_mrr"], bottom=mrr["churn_mrr"],
           label="Contraction", color=COLORS["contraction"], alpha=0.85, width=0.8)

    ax.axhline(y=0, color="black", linewidth=0.8)
    ax.set_title("MRR Movements by Type", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("MRR Change ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(loc="upper left", fontsize=8, ncol=3)
    ax.set_xticks(range(0, len(mrr), 6))
    ax.set_xticklabels(mrr["month_id"].iloc[::6], rotation=45, ha="right")



# Chart 3: Cohort Retention Curves
# Each line is a group of customers who signed up in the same month.
# The y-axis shows what % are still paying N months later.
# Healthy curves flatten out around 60-70%. Curves that keep dropping
# toward zero mean the product doesn't retain users.

def plot_cohort_retention(cohort, ax):
    # Pick quarterly cohorts for readability (otherwise too many lines)
    quarterly_cohorts = sorted([
        c for c in cohort["signup_cohort"].unique()
        if c.endswith(("-01", "-04", "-07", "-10"))
    ])

    # Only show cohorts that have at least 6 months of data
    selected = [c for c in quarterly_cohorts
                if cohort[cohort["signup_cohort"] == c]["months_since_signup"].max() >= 6]

    colors = np.linspace(0.1, 0.9, len(selected))
    cmap = plt.cm.viridis(colors)

    for i, cohort_name in enumerate(selected):
        cohort_data = cohort[cohort["signup_cohort"] == cohort_name].sort_values("months_since_signup")
        if len(cohort_data) > 1:
            ax.plot(
                cohort_data["months_since_signup"],
                cohort_data["retention_rate"],
                label=cohort_name, color=cmap[i], linewidth=1.5, alpha=0.8
            )

    ax.set_title("Cohort Retention Curves", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Months Since Signup")
    ax.set_ylabel("Retention Rate (%)")
    ax.set_ylim(0, 105)
    ax.set_xlim(0, 24)
    ax.legend(loc="lower left", fontsize=7, ncol=2, title="Cohort", title_fontsize=8)



# Chart 4: Customer Health Distribution
# A snapshot of the current customer base segmented by health score.
# Champions and stable customers are your foundation.
# At-risk customers need attention from the CS team.

def plot_customer_health(ltv, ax):
    health_order = ["champion", "stable", "new", "at_risk", "churned"]
    health_colors = {
        "champion": COLORS["positive"],
        "stable": COLORS["primary"],
        "new": COLORS["expansion"],
        "at_risk": COLORS["warning"],
        "churned": COLORS["negative"],
    }

    counts = ltv["customer_health"].value_counts()
    counts = counts.reindex(health_order).fillna(0)

    bars = ax.barh(
        counts.index, counts.values,
        color=[health_colors.get(h, COLORS["neutral"]) for h in counts.index],
        alpha=0.85, edgecolor="white", linewidth=0.5
    )

    # Add count labels to each bar
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
                f"{int(val)}", va="center", fontsize=10, fontweight="bold")

    ax.set_title("Customer Health Distribution", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Number of Customers")
    ax.invert_yaxis()  # champion on top



# Chart 5: ARPU Trend
# Average Revenue Per User. If this is going up, it means customers
# are upgrading to higher plans over time — a sign of healthy expansion.

def plot_arpu_trend(mrr, ax):
    ax.plot(range(len(mrr)), mrr["arpu"], color=COLORS["expansion"],
            linewidth=2.5, marker="o", markersize=3)

    ax.set_title("Average Revenue Per User (ARPU)", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("ARPU ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xticks(range(0, len(mrr), 6))
    ax.set_xticklabels(mrr["month_id"].iloc[::6], rotation=45, ha="right")
    ax.set_xlim(0, len(mrr) - 1)



# Chart 6: Churn Rate Trend
# Shows both customer churn (% of customers lost) and revenue churn
# (% of MRR lost). Revenue churn can be higher than customer churn
# if you're losing big customers, or lower if you're only losing
# small ones. The trendline shows the overall direction.

def plot_churn_rate(mrr, ax):
    x_vals = range(len(mrr))

    ax.plot(x_vals, mrr["gross_customer_churn_rate"],
            color=COLORS["negative"], linewidth=2, label="Customer Churn %")
    ax.plot(x_vals, mrr["gross_revenue_churn_rate"],
            color=COLORS["warning"], linewidth=2, linestyle="--", label="Revenue Churn %")

    # Add a trend line so the overall direction is clear
    x_array = np.arange(len(mrr))
    trend_coeffs = np.polyfit(x_array, mrr["gross_customer_churn_rate"].fillna(0), 1)
    ax.plot(x_array, np.polyval(trend_coeffs, x_array),
            color=COLORS["negative"], linewidth=1, alpha=0.4, linestyle=":")

    ax.set_title("Monthly Churn Rates", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Churn Rate (%)")
    ax.legend(loc="upper right", fontsize=9)
    ax.set_xticks(range(0, len(mrr), 6))
    ax.set_xticklabels(mrr["month_id"].iloc[::6], rotation=45, ha="right")
    ax.set_xlim(0, len(mrr) - 1)


def print_summary(mrr, ltv):
    """Print a text summary of key metrics to the terminal."""
    latest = mrr.iloc[-1]
    first = mrr.iloc[0]
    mrr_growth = ((latest["total_mrr"] - first["total_mrr"]) / first["total_mrr"]) * 100
    active_ltv = ltv[ltv["current_status"] == "active"]

    print()
    print("=" * 55)
    print("  ACME ANALYTICS — KEY METRICS SUMMARY")
    print(f"  Period: {mrr['month_id'].iloc[0]} to {mrr['month_id'].iloc[-1]}")
    print("=" * 55)
    print(f"  Current MRR:       ${latest['total_mrr']:>10,.0f}")
    print(f"  MRR Growth:        {mrr_growth:>10.1f}%")
    print(f"  Active Customers:  {latest['active_customers']:>10,.0f}")
    print(f"  ARPU:              ${latest['arpu']:>10,.2f}")
    print(f"  Avg Churn Rate:    {mrr['gross_customer_churn_rate'].mean():>10.2f}%")
    print(f"  Avg LTV (active):  ${active_ltv['estimated_ltv'].mean():>10,.0f}")
    print("=" * 55)
    print()


def main():
    print("Loading data from pipeline...")
    mrr, cohort, ltv = load_data()

    # Print text summary
    print_summary(mrr, ltv)

    # Build the 6-panel dashboard
    fig, axes = plt.subplots(3, 2, figsize=(16, 18))
    fig.suptitle(
        "Acme Analytics — SaaS Metrics Dashboard",
        fontsize=18, fontweight="bold", y=0.98
    )

    plot_mrr_growth(mrr, axes[0, 0])
    plot_mrr_waterfall(mrr, axes[0, 1])
    plot_cohort_retention(cohort, axes[1, 0])
    plot_customer_health(ltv, axes[1, 1])
    plot_arpu_trend(mrr, axes[2, 0])
    plot_churn_rate(mrr, axes[2, 1])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Saving dashboard
    output_path = os.path.join(OUTPUT_DIR, "saas_metrics_dashboard.png")
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()

    print(f"Dashboard saved to: {output_path}")


if __name__ == "__main__":
    main()