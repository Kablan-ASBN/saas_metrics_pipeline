-- stg_plans.sql
-- Standardizes the plan reference table.
-- The plan_tier column gives us a numeric ranking we can use
-- to determine if a plan change was an upgrade or downgrade.

SELECT
    plan_id,
    LOWER(TRIM(plan_name)) AS plan_name,
    monthly_price,
    ROW_NUMBER() OVER (ORDER BY monthly_price) AS plan_tier
FROM raw_plans