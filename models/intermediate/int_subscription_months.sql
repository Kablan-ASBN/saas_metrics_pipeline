-- int_subscription_months.sql
-- This is the backbone of the entire pipeline.
--
-- The problem: our raw data only tells us WHEN things happened (signup, churn, upgrade).
-- But to calculate MRR for any given month, we need to know WHO was active and
-- WHAT they were paying in that month — even if nothing happened that month.
--
-- The solution: create one row per subscription per active month. If a customer
-- signed up in January and churned in June, they get rows for Jan, Feb, Mar, Apr, May, Jun.
-- Each row carries the correct MRR for that month (accounting for any mid-period upgrades).
--
-- If you've used dbt at a SaaS company, you've probably seen a model like this.
-- It's sometimes called a "subscription spine" or "monthly grain" table.

WITH months AS (
    -- Get every distinct month that appears in our event data.
    -- This becomes the "calendar" we join against.
    SELECT DISTINCT STRFTIME('%Y-%m', event_date) AS month_id
    FROM stg_events
    ORDER BY month_id
),

subscription_months AS (
    -- For each subscription, figure out which months it was active.
    -- We cross join with the months calendar, then filter to only keep
    -- months that fall between the sub's start and end date.
    SELECT
        s.subscription_id,
        s.customer_id,
        s.plan_id AS original_plan_id,
        m.month_id,
        s.start_date,
        s.end_date
    FROM stg_subscriptions s
    CROSS JOIN months m
    WHERE m.month_id >= STRFTIME('%Y-%m', s.start_date)
      AND m.month_id <= STRFTIME('%Y-%m', COALESCE(s.end_date, '2024-12-31'))
),

-- For months where an event happened (upgrade, downgrade, etc),
-- we need to know what the MRR was AFTER that event. If multiple
-- events happened in the same month, we take the latest one.
latest_event_per_month AS (
    SELECT
        subscription_id,
        event_month,
        MAX(event_id) AS latest_event_id
    FROM stg_events
    GROUP BY subscription_id, event_month
),

event_mrr AS (
    SELECT
        e.subscription_id,
        e.event_month,
        e.new_mrr,
        e.new_plan_id,
        e.event_type
    FROM stg_events e
    INNER JOIN latest_event_per_month le
        ON e.event_id = le.latest_event_id
)

SELECT
    sm.subscription_id,
    sm.customer_id,
    sm.month_id,

    -- Here's the tricky part: what MRR should this row have?
    -- If there was an event this month, use that event's new_mrr.
    -- If not, we need to "carry forward" the MRR from the most recent event.
    COALESCE(
        em.new_mrr,
        (SELECT COALESCE(e2.new_mrr, sub.mrr)
         FROM stg_subscriptions sub
         LEFT JOIN stg_events e2
            ON e2.subscription_id = sm.subscription_id
            AND e2.event_month <= sm.month_id
            AND e2.event_type != 'churn'
         WHERE sub.subscription_id = sm.subscription_id
         ORDER BY e2.event_date DESC
         LIMIT 1)
    ) AS mrr,

    COALESCE(em.new_plan_id, sm.original_plan_id) AS plan_id,

    -- These flags are useful downstream for counting new vs churned customers
    CASE WHEN sm.month_id = STRFTIME('%Y-%m', sm.start_date)
        THEN 1 ELSE 0
    END AS is_first_month,

    CASE WHEN sm.end_date IS NOT NULL
         AND sm.month_id = STRFTIME('%Y-%m', sm.end_date)
        THEN 1 ELSE 0
    END AS is_churn_month

FROM subscription_months sm
LEFT JOIN event_mrr em
    ON sm.subscription_id = em.subscription_id
    AND sm.month_id = em.event_month

ORDER BY sm.customer_id, sm.month_id