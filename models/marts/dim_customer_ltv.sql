-- dim_customer_ltv.sql
-- A single row per customer with their full story: what they're paying now,
-- how long they've been around, how much total revenue they've generated,
-- and a simple health score.
--
-- The "dim_" prefix means this is a dimension table (describes an entity)
-- vs "fct_" which is a fact table (describes events/measurements).
--
-- The LTV estimate here is deliberately simple: historical revenue plus
-- a 6-month forward projection at their average MRR. A real production model
-- would use survival curves (Kaplan-Meier) or a probabilistic model (BG/NBD).
-- I mention this in the README under "Known Limitations."

WITH customer_revenue AS (
    -- Aggregate the subscription spine to get lifetime revenue stats
    SELECT
        customer_id,
        SUM(mrr) AS total_lifetime_revenue,
        COUNT(DISTINCT month_id) AS active_months,
        AVG(mrr) AS avg_monthly_mrr,
        MAX(mrr) AS peak_mrr,
        MIN(month_id) AS first_month,
        MAX(month_id) AS last_month
    FROM int_subscription_months
    GROUP BY customer_id
),

customer_events AS (
    -- Count lifecycle events, useful for understanding behavior patterns
    SELECT
        customer_id,
        COUNT(CASE WHEN event_type = 'upgrade' THEN 1 END) AS upgrade_count,
        COUNT(CASE WHEN event_type = 'downgrade' THEN 1 END) AS downgrade_count,
        COUNT(CASE WHEN event_type = 'churn' THEN 1 END) AS churn_count,
        COUNT(CASE WHEN event_type = 'reactivation' THEN 1 END) AS reactivation_count
    FROM stg_events
    GROUP BY customer_id
),

current_status AS (
    -- Get each customer's most recent subscription to determine current state.
    -- The MAX(subscription_id) trick works because subscription IDs are sequential.
    SELECT
        customer_id,
        subscription_status,
        plan_id,
        mrr AS current_mrr
    FROM stg_subscriptions
    WHERE subscription_id IN (
        SELECT MAX(subscription_id)
        FROM stg_subscriptions
        GROUP BY customer_id
    )
)

SELECT
    c.customer_id,
    c.customer_name,
    c.signup_cohort,
    c.industry,
    c.company_size,

    -- Where are they now?
    cs.subscription_status AS current_status,
    p.plan_name AS current_plan,
    cs.current_mrr,

    -- Lifetime numbers
    cr.total_lifetime_revenue,
    cr.active_months,
    cr.avg_monthly_mrr,
    cr.peak_mrr,
    cr.first_month,
    cr.last_month,

    -- Behavioral signals
    COALESCE(ce.upgrade_count, 0) AS upgrade_count,
    COALESCE(ce.downgrade_count, 0) AS downgrade_count,
    COALESCE(ce.churn_count, 0) AS churn_count,
    COALESCE(ce.reactivation_count, 0) AS reactivation_count,

    -- Simple health score based on tenure and behavior.
    -- Not sophisticated, but gives you a quick way to segment the customer base.
    CASE
        WHEN cs.subscription_status = 'churned' THEN 'churned'
        WHEN ce.downgrade_count > 0 AND ce.downgrade_count > ce.upgrade_count THEN 'at_risk'
        WHEN cr.active_months >= 12 AND ce.upgrade_count > 0 THEN 'champion'
        WHEN cr.active_months >= 6 THEN 'stable'
        ELSE 'new'
    END AS customer_health,

    -- LTV estimate: historical revenue + 6-month projection for active customers.
    -- Churned customers just get their historical total.
    CASE
        WHEN cs.subscription_status = 'active'
        THEN cr.total_lifetime_revenue + (cr.avg_monthly_mrr * 6)
        ELSE cr.total_lifetime_revenue
    END AS estimated_ltv

FROM stg_customers c
LEFT JOIN customer_revenue cr USING (customer_id)
LEFT JOIN customer_events ce USING (customer_id)
LEFT JOIN current_status cs USING (customer_id)
LEFT JOIN stg_plans p ON cs.plan_id = p.plan_id
ORDER BY estimated_ltv DESC