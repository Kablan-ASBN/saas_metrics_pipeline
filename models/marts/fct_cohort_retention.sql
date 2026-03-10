-- fct_cohort_retention.sql
-- Cohort retention analysis: for each group of customers who signed up in the
-- same month (a "cohort"), what percentage are still active N months later?
--
-- This is the table behind retention curve charts. Each row is one cohort
-- at one point in time. For example: "Of the 22 customers who signed up in
-- 2022-01, 15 were still active 9 months later (68.18% retention)."
--
-- Why this matters: retention curves that flatten out (stop dropping) mean
-- you have product-market fit. Curves that keep falling mean you have a
-- leaky bucket problem no amount of new signups can fix.

WITH customer_cohorts AS (
    -- Link each customer to their signup cohort and first active month
    SELECT
        customer_id,
        signup_cohort,
        MIN(month_id) AS first_active_month
    FROM stg_customers c
    INNER JOIN int_subscription_months sm USING (customer_id)
    GROUP BY customer_id, signup_cohort
),

cohort_activity AS (
    -- For each customer, calculate how many months after signup they were active.
    -- A customer active in their signup month has months_since_signup = 0.
    SELECT
        cc.signup_cohort,
        cc.customer_id,
        sm.month_id,
        (CAST(SUBSTR(sm.month_id, 1, 4) AS INTEGER) - CAST(SUBSTR(cc.signup_cohort, 1, 4) AS INTEGER)) * 12
        + (CAST(SUBSTR(sm.month_id, 6, 2) AS INTEGER) - CAST(SUBSTR(cc.signup_cohort, 6, 2) AS INTEGER))
        AS months_since_signup
    FROM customer_cohorts cc
    INNER JOIN int_subscription_months sm
        ON cc.customer_id = sm.customer_id
    WHERE sm.is_churn_month = 0
),

cohort_sizes AS (
    -- How many customers were in each cohort at the start?
    -- This is the denominator for retention rate.
    SELECT
        signup_cohort,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY signup_cohort
)

SELECT
    ca.signup_cohort,
    ca.months_since_signup,
    cs.cohort_size,
    COUNT(DISTINCT ca.customer_id) AS retained_customers,
    ROUND(
        COUNT(DISTINCT ca.customer_id) * 100.0 / cs.cohort_size,
        2
    ) AS retention_rate
FROM cohort_activity ca
INNER JOIN cohort_sizes cs USING (signup_cohort)
WHERE ca.months_since_signup >= 0
GROUP BY ca.signup_cohort, ca.months_since_signup, cs.cohort_size
ORDER BY ca.signup_cohort, ca.months_since_signup