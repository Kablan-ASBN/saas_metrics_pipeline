-- stg_events.sql
-- Cleans up the event log and adds a pre-calculated mrr_delta column.
-- This saves us from doing (new_mrr - old_mrr) over and over in downstream models.

SELECT
    event_id,
    subscription_id,
    customer_id,
    DATE(event_date) AS event_date,
    STRFTIME('%Y-%m', event_date) AS event_month,
    LOWER(TRIM(event_type)) AS event_type,
    old_plan_id,
    new_plan_id,
    COALESCE(old_mrr, 0) AS old_mrr,
    COALESCE(new_mrr, 0) AS new_mrr,
    COALESCE(new_mrr, 0) - COALESCE(old_mrr, 0) AS mrr_delta
FROM raw_events