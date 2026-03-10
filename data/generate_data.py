"""
generate_data.py
----------------
Creates synthetic subscription data for a fictional B2B SaaS company called "Acme Analytics".

Why synthetic data?
    I wanted anyone to be able to clone this repo and run the full pipeline without
    needing API keys or downloading external datasets. The tradeoff is realism —
    but I've tried to model patterns that actually show up in real SaaS businesses:
    
    - Signups spike in Q1 (companies spending new budgets) and Q3 (back-to-school season)
    - Most customers start on mid-tier plans, not the cheapest or most expensive
    - Churn is highest in months 2-4 (the "aha moment" window)
    - Upgrades happen more often after month 3, once customers see value
    - Some churned customers come back (reactivation), usually at the same or lower tier

The script outputs 4 CSV files that act as the "raw" source layer for the pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import random
import os

# Set seeds so the output is the same every time you run it.
# This matters because the analysis and README reference specific numbers.
np.random.seed(42)
random.seed(42)


# --- Company configuration ---
# These are the knobs you'd tweak if you wanted to simulate a different kind of business.

NUM_CUSTOMERS = 800
DATE_START = datetime(2022, 1, 1)
DATE_END = datetime(2024, 12, 31)

# Pricing tiers: typical B2B SaaS structure with 4x jump from bottom to top
PLANS = [
    {"plan_id": 1, "plan_name": "Starter",      "monthly_price": 29},
    {"plan_id": 2, "plan_name": "Professional",  "monthly_price": 79},
    {"plan_id": 3, "plan_name": "Business",      "monthly_price": 199},
    {"plan_id": 4, "plan_name": "Enterprise",    "monthly_price": 499},
]

INDUSTRIES = [
    "Technology", "Healthcare", "Finance", "Retail", "Education",
    "Manufacturing", "Media", "Real Estate", "Consulting", "Logistics"
]

COMPANY_SIZES = ["1-10", "11-50", "51-200", "201-500", "501-1000", "1000+"]

# Most customers sign up for Professional (the sweet spot).
# Very few go straight to Enterprise; that's realistic for self-serve SaaS.
PLAN_SIGNUP_WEIGHTS = [0.30, 0.40, 0.22, 0.08]

# Smaller companies are more common in self-serve SaaS
SIZE_WEIGHTS = [0.15, 0.30, 0.25, 0.15, 0.10, 0.05]


def generate_customers(n):
    """
    Create n customer records with signup dates spread across the analysis period.
    
    The signup dates aren't uniform — they follow seasonal patterns:
    - Jan/Feb: 60% more signups (new year budgets)
    - Aug/Sep: 30% more signups (Q3 planning)  
    - Nov/Dec: 30% fewer signups (holiday slowdown)
    
    There's also a growth trend baked in: later months get more signups
    than earlier months, simulating a growing company.
    """
    customers = []
    
    # Build a list of all months in our analysis window
    all_months = pd.date_range(DATE_START, DATE_END, freq="MS")
    
    # Assign a relative weight to each month based on seasonality + growth
    month_weights = []
    for month in all_months:
        # Start with a baseline weight of 1.0
        weight = 1.0
        
        # Seasonal adjustments
        if month.month in [1, 2]:
            weight = 1.6       # Q1 budget flush
        elif month.month in [8, 9]:
            weight = 1.3       # Q3 ramp-up
        elif month.month in [11, 12]:
            weight = 0.7       # holiday slowdown
        
        # Growth trend: company gets bigger over time, so later months
        # should have more signups. This creates a ~80% increase from
        # the first month to the last.
        months_elapsed = (month.year - DATE_START.year) * 12 + month.month - DATE_START.month
        growth_multiplier = 1 + (months_elapsed / len(all_months)) * 0.8
        
        month_weights.append(weight * growth_multiplier)
    
    # Normalize weights so they sum to 1 (required for np.random.choice)
    month_weights = np.array(month_weights) / sum(month_weights)
    
    # Now generate each customer
    for i in range(1, n + 1):
        # Pick a signup month based on our weighted distribution
        signup_month = pd.Timestamp(np.random.choice(all_months, p=month_weights))
        
        # Pick a random day within that month (cap at 28 to avoid month-length issues)
        signup_day = random.randint(1, 28)
        signup_date = signup_month.replace(day=signup_day)
        
        customers.append({
            "customer_id": i,
            "customer_name": f"Company_{i:04d}",
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "industry": random.choice(INDUSTRIES),
            "company_size": random.choices(COMPANY_SIZES, weights=SIZE_WEIGHTS, k=1)[0],
        })
    
    return pd.DataFrame(customers)


def generate_subscription_lifecycle(customers_df):
    """
    For each customer, simulate their journey through the subscription lifecycle:
    signup -> (possible upgrades/downgrades) -> (possible churn) -> (possible reactivation)
    
    This is the most complex part of the generator. Each customer gets walked through
    month by month, and at each step we roll the dice on whether they churn, upgrade,
    or downgrade. The probabilities are tuned to produce realistic-looking metrics:
    
    - ~4% average monthly churn rate
    - Churn peaks in months 2-4 (before customers fully adopt the product)
    - Starter plan customers churn 50% more (less invested)
    - Enterprise customers churn 60% less (bigger commitment, more integrations)
    - ~15% of churned customers reactivate within 1-3 months
    
    Returns two dataframes:
    - subscriptions: one row per subscription period (start/end dates, plan, MRR)
    - events: one row per lifecycle event (new, upgrade, downgrade, churn, reactivation)
    """
    # Quick lookup: plan_id -> monthly price
    plan_prices = {p["plan_id"]: p["monthly_price"] for p in PLANS}
    
    subscriptions = []
    events = []
    sub_id = 0
    event_id = 0
    
    for _, customer in customers_df.iterrows():
        cust_id = customer["customer_id"]
        signup = pd.Timestamp(customer["signup_date"])
        current_date = signup
        
        # Pick their initial plan (weighted toward Professional)
        initial_plan = random.choices(
            [p["plan_id"] for p in PLANS],
            weights=PLAN_SIGNUP_WEIGHTS,
            k=1
        )[0]
        
        # Set up their first subscription
        sub_id += 1
        current_sub_id = sub_id
        current_plan = initial_plan
        current_mrr = plan_prices[current_plan]
        sub_start = current_date
        
        # Record the "new subscription" event
        event_id += 1
        events.append({
            "event_id": event_id,
            "subscription_id": current_sub_id,
            "customer_id": cust_id,
            "event_date": current_date.strftime("%Y-%m-%d"),
            "event_type": "new",
            "old_plan_id": None,
            "new_plan_id": current_plan,
            "old_mrr": 0,
            "new_mrr": current_mrr,
        })
        
        # Now walk through each subsequent month and simulate what happens
        active = True
        months_active = 0
        
        while current_date <= pd.Timestamp(DATE_END) and active:
            current_date += pd.DateOffset(months=1)
            if current_date > pd.Timestamp(DATE_END):
                break
            months_active += 1
            
            # -------------------------------------------------------
            # CHURN CHECK
            # The probability depends on how long they've been a customer
            # and what plan they're on. This creates the classic "bathtub curve"
            # where churn is high early, drops off, then stabilizes.
            # -------------------------------------------------------
            if months_active <= 2:
                churn_prob = 0.06       # still figuring out the product
            elif months_active <= 4:
                churn_prob = 0.08       # the "make or break" window
            elif months_active <= 8:
                churn_prob = 0.04       # settling in
            else:
                churn_prob = 0.02       # loyal customer, low risk
            
            # Plan-based adjustments
            if current_plan == 1:       # Starter customers are less committed
                churn_prob *= 1.5
            elif current_plan == 4:     # Enterprise customers are locked in
                churn_prob *= 0.4
            
            if random.random() < churn_prob:
                # Customer churns — record the event and close the subscription
                event_id += 1
                events.append({
                    "event_id": event_id,
                    "subscription_id": current_sub_id,
                    "customer_id": cust_id,
                    "event_date": current_date.strftime("%Y-%m-%d"),
                    "event_type": "churn",
                    "old_plan_id": current_plan,
                    "new_plan_id": None,
                    "old_mrr": current_mrr,
                    "new_mrr": 0,
                })
                
                subscriptions.append({
                    "subscription_id": current_sub_id,
                    "customer_id": cust_id,
                    "plan_id": current_plan,
                    "start_date": sub_start.strftime("%Y-%m-%d"),
                    "end_date": current_date.strftime("%Y-%m-%d"),
                    "mrr": current_mrr,
                })
                
                # Some customers come back! About 15% reactivate within 1-3 months.
                # When they do, they often come back at the same or a lower tier.
                if random.random() < 0.15:
                    gap_months = random.randint(1, 3)
                    current_date += pd.DateOffset(months=gap_months)
                    
                    if current_date > pd.Timestamp(DATE_END):
                        active = False
                        break
                    
                    # Pick reactivation plan: 40% chance they downgrade, 60% same plan
                    reactivate_plan = random.choices(
                        [max(1, current_plan - 1), current_plan],
                        weights=[0.4, 0.6],
                        k=1
                    )[0]
                    
                    # Start a new subscription
                    sub_id += 1
                    current_sub_id = sub_id
                    current_plan = reactivate_plan
                    current_mrr = plan_prices[current_plan]
                    sub_start = current_date
                    months_active = 0  # reset tenure for the new subscription
                    
                    event_id += 1
                    events.append({
                        "event_id": event_id,
                        "subscription_id": current_sub_id,
                        "customer_id": cust_id,
                        "event_date": current_date.strftime("%Y-%m-%d"),
                        "event_type": "reactivation",
                        "old_plan_id": None,
                        "new_plan_id": current_plan,
                        "old_mrr": 0,
                        "new_mrr": current_mrr,
                    })
                else:
                    active = False  # gone for good
                continue
            
            # -------------------------------------------------------
            # UPGRADE CHECK
            # Customers who've been around 3+ months and are seeing value
            # are candidates for upgrading. Lower-tier customers are
            # slightly more likely to upgrade (more room to grow).
            # -------------------------------------------------------
            if current_plan < 4 and months_active >= 3:
                upgrade_prob = 0.04 if months_active < 8 else 0.06
                if current_plan == 1:
                    upgrade_prob *= 1.3  # Starter users who stuck around are ready
                
                if random.random() < upgrade_prob:
                    old_plan = current_plan
                    old_mrr = current_mrr
                    current_plan = min(current_plan + 1, 4)  # move up one tier
                    current_mrr = plan_prices[current_plan]
                    
                    event_id += 1
                    events.append({
                        "event_id": event_id,
                        "subscription_id": current_sub_id,
                        "customer_id": cust_id,
                        "event_date": current_date.strftime("%Y-%m-%d"),
                        "event_type": "upgrade",
                        "old_plan_id": old_plan,
                        "new_plan_id": current_plan,
                        "old_mrr": old_mrr,
                        "new_mrr": current_mrr,
                    })
                    continue
            
            # -------------------------------------------------------
            # DOWNGRADE CHECK
            # Less common than upgrades. Usually a warning sign that
            # churn might follow. Only happens after month 2.
            # -------------------------------------------------------
            if current_plan > 1 and months_active >= 2:
                downgrade_prob = 0.02
                
                if random.random() < downgrade_prob:
                    old_plan = current_plan
                    old_mrr = current_mrr
                    current_plan = max(current_plan - 1, 1)  # move down one tier
                    current_mrr = plan_prices[current_plan]
                    
                    event_id += 1
                    events.append({
                        "event_id": event_id,
                        "subscription_id": current_sub_id,
                        "customer_id": cust_id,
                        "event_date": current_date.strftime("%Y-%m-%d"),
                        "event_type": "downgrade",
                        "old_plan_id": old_plan,
                        "new_plan_id": current_plan,
                        "old_mrr": old_mrr,
                        "new_mrr": current_mrr,
                    })
        
        # If the customer is still active at the end of our analysis window,
        # close out their subscription record with no end date (means "still active")
        if active:
            subscriptions.append({
                "subscription_id": current_sub_id,
                "customer_id": cust_id,
                "plan_id": current_plan,
                "start_date": sub_start.strftime("%Y-%m-%d"),
                "end_date": None,
                "mrr": current_mrr,
            })
    
    return pd.DataFrame(subscriptions), pd.DataFrame(events)


def main():
    print("Generating synthetic SaaS data for Acme Analytics...")
    print(f"  Period: {DATE_START.strftime('%Y-%m-%d')} to {DATE_END.strftime('%Y-%m-%d')}")
    print(f"  Customers: {NUM_CUSTOMERS}")
    print()
    
    # Where to save the CSVs (same directory as this script)
    output_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Generate each table
    plans_df = pd.DataFrame(PLANS)
    customers_df = generate_customers(NUM_CUSTOMERS)
    subscriptions_df, events_df = generate_subscription_lifecycle(customers_df)
    
    # Save to CSV
    plans_df.to_csv(os.path.join(output_dir, "raw_plans.csv"), index=False)
    customers_df.to_csv(os.path.join(output_dir, "raw_customers.csv"), index=False)
    subscriptions_df.to_csv(os.path.join(output_dir, "raw_subscriptions.csv"), index=False)
    events_df.to_csv(os.path.join(output_dir, "raw_events.csv"), index=False)
    
    # Print a summary so we know everything looks right
    print("Results:")
    print(f"  Customers:     {len(customers_df):,}")
    print(f"  Subscriptions: {len(subscriptions_df):,}")
    print(f"  Events:        {len(events_df):,}")
    print(f"  Still active:  {subscriptions_df['end_date'].isna().sum():,}")
    print()
    print("Event breakdown:")
    for event_type, count in events_df["event_type"].value_counts().items():
        print(f"  {event_type:15s} {count:,}")
    print()
    print(f"Files saved to: {output_dir}")


if __name__ == "__main__":
    main()