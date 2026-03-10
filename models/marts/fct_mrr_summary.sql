-- fct_mrr_summary.sql
-- The monthly MRR waterfall, arguably the most important table in any SaaS data stack.
--
-- Each row is one month. It shows:
-- - Total MRR and how many customers are active
-- - How that MRR changed vs last month, broken down by movement type
-- - Customer and revenue churn rates
-- - ARPU (average revenue per user)
--
-- This is the table behind the MRR growth chart, the waterfall chart,
-- and most of the headline numbers in a board deck or investor update.

WITH monthly_movements AS (
    -- Aggregate all MRR movements by month
    SELECT
        event_month,
        SUM(CASE WHEN event_type = 'new' THEN new_mrr_amount ELSE 0 END) AS new_mrr,
        SUM(expansion_mrr_amount) AS expansion_mrr,
        SUM(contraction_mrr_amount) AS contraction_mrr,
        SUM(churn_mrr_amount) AS churn_mrr,
        SUM(reactivation_mrr_amount) AS reactivation_mrr,
        COUNT(DISTINCT CASE WHEN event_type = 'new' THEN customer_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN event_type = 'churn' THEN customer_id END) AS churned_customers,
        COUNT(DISTINCT CASE WHEN event_type = 'reactivation' THEN customer_id END) AS reactivated_customers
    FROM int_mrr_movements
    GROUP BY event_month
),

-- Get the actual MRR total from the subscription spine.
-- We exclude churn-month rows because those customers aren't really
-- "active" that month, they cancelled during it.
active_mrr AS (
    SELECT
        month_id,
        SUM(mrr) AS total_mrr,
        COUNT(DISTINCT customer_id) AS active_customers,
        COUNT(DISTINCT subscription_id) AS active_subscriptions
    FROM int_subscription_months
    WHERE is_churn_month = 0
    GROUP BY month_id
)

SELECT
    a.month_id,
    a.total_mrr,
    a.active_customers,
    a.active_subscriptions,
    ROUND(a.total_mrr * 1.0 / NULLIF(a.active_customers, 0), 2) AS arpu,

    -- MRR waterfall components
    COALESCE(m.new_mrr, 0) AS new_mrr,
    COALESCE(m.expansion_mrr, 0) AS expansion_mrr,
    COALESCE(m.contraction_mrr, 0) AS contraction_mrr,
    COALESCE(m.churn_mrr, 0) AS churn_mrr,
    COALESCE(m.reactivation_mrr, 0) AS reactivation_mrr,

    -- Net change = sum of all movements. Positive means we grew.
    COALESCE(m.new_mrr, 0) + COALESCE(m.expansion_mrr, 0)
    + COALESCE(m.contraction_mrr, 0) + COALESCE(m.churn_mrr, 0)
    + COALESCE(m.reactivation_mrr, 0) AS net_mrr_change,

    -- Customer movement counts
    COALESCE(m.new_customers, 0) AS new_customers,
    COALESCE(m.churned_customers, 0) AS churned_customers,
    COALESCE(m.reactivated_customers, 0) AS reactivated_customers,

    -- Gross customer churn rate
    -- "What % of customers at the start of the month did we lose?"
    -- The denominator approximates start-of-month by adding back churned customers.
    ROUND(
        COALESCE(m.churned_customers, 0) * 100.0
        / NULLIF(a.active_customers + COALESCE(m.churned_customers, 0), 0),
        2
    ) AS gross_customer_churn_rate,

    -- Gross revenue churn rate
    -- Same idea but for dollars instead of headcount.
    ROUND(
        ABS(COALESCE(m.churn_mrr, 0)) * 100.0
        / NULLIF(a.total_mrr + ABS(COALESCE(m.churn_mrr, 0)), 0),
        2
    ) AS gross_revenue_churn_rate

FROM active_mrr a
LEFT JOIN monthly_movements m ON a.month_id = m.event_month
ORDER BY a.month_id