-- int_mrr_movements.sql
-- Takes each subscription event and breaks its MRR impact into the 5 standard
-- categories that every SaaS company tracks:
--
--   new_mrr          → first-time revenue from a brand new customer
--   expansion_mrr    → revenue gained when a customer upgrades
--   contraction_mrr  → revenue lost when a customer downgrades (but stays)
--   churn_mrr        → revenue lost when a customer cancels entirely
--   reactivation_mrr → revenue from a customer who previously cancelled and came back
--
-- These categories are the building blocks of the MRR waterfall chart,
-- which is probably the single most common chart in SaaS board decks.

SELECT
    event_id,
    subscription_id,
    customer_id,
    event_date,
    event_month,
    event_type,
    old_mrr,
    new_mrr,
    mrr_delta,

    -- Break out each movement type into its own column.
    -- This makes the downstream aggregation in fct_mrr_summary much cleaner.

    CASE event_type
        WHEN 'new'          THEN new_mrr
        WHEN 'reactivation' THEN new_mrr
        ELSE 0
    END AS new_mrr_amount,

    CASE event_type
        WHEN 'upgrade' THEN mrr_delta
        ELSE 0
    END AS expansion_mrr_amount,

    CASE event_type
        WHEN 'downgrade' THEN mrr_delta   -- this will be negative
        ELSE 0
    END AS contraction_mrr_amount,

    CASE event_type
        WHEN 'churn' THEN -old_mrr        -- lost revenue is stored as negative
        ELSE 0
    END AS churn_mrr_amount,

    CASE event_type
        WHEN 'reactivation' THEN new_mrr
        ELSE 0
    END AS reactivation_mrr_amount


FROM stg_events