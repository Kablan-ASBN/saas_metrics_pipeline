-- stg_subscriptions.sql
-- Adds a couple of useful columns to the raw subscription data:
-- 1. subscription_status — is this sub still active or did they cancel?
-- 2. tenure_months — how long were they subscribed? For active subs,
--    we measure up to the end of our analysis period (2024-12-31).

SELECT
    subscription_id,
    customer_id,
    plan_id,
    DATE(start_date) AS start_date,
    DATE(end_date) AS end_date,
    mrr,
    CASE
        WHEN end_date IS NULL THEN 'active'
        ELSE 'churned'
    END AS subscription_status,
    CAST(
        (JULIANDAY(COALESCE(end_date, '2024-12-31')) - JULIANDAY(start_date)) / 30.44
        AS INTEGER
    ) AS tenure_months
FROM raw_subscriptions