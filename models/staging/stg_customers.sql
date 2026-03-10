-- stg_customers.sql
-- Cleans up the raw customer table. Not much to do here honestly —
-- just standardizing the industry field and extracting a signup cohort
-- (year-month) that we'll need later for retention analysis.

SELECT
    customer_id,
    customer_name,
    DATE(signup_date) AS signup_date,
    LOWER(TRIM(industry)) AS industry,
    company_size,
    STRFTIME('%Y-%m', signup_date) AS signup_cohort
FROM raw_customers