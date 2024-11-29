select count(*) from funds_investo2o.transactions ;
WITH cte0 AS (
  SELECT
    a.asset_id as asset_id,
    ksg.kristal_execution_account as user_account_id,
    a.subscription_goal_id as subscription_goal_id,
    a.transaction_id as transaction_id,
    trunc(
      trim_scale(ksg.approved_units),
      4
    ) as approved_units
  from
    funds_orders.fund_order_info as a
    INNER JOIN funds_kristals.kristal_subscription_goal as ksg on ksg.kristal_subscription_goal_id = a.subscription_goal_id
    INNER JOIN kristaldata_kristals.kristal_properties as kp ON kp.kristal_id = ksg.kristal_id
    and a.asset_id = kp.lone_asset_id
),
cte1 AS (
  SELECT
    t.* ,
    foi.subscription_goal_id AS foi_goal_id,
    concat_ws (
      '|',
      trunc(
        trim_scale(
          ROUND(t.quantity :: numeric, 10)
        ),
        4
      ),
      t.user_account_id,
      t.asset_id
    ) AS txn_key,
    date(t.trade_time) ttime,
    date(t.created_time) tctime
  FROM
    funds_investo2o.transactions as t
    LEFT OUTER JOIN cte0 as foi ON t.user_account_id = foi.user_account_id
    and foi.transaction_id = t.external_transaction_id
    and foi.approved_units = trunc(
      trim_scale(
        ROUND(t.quantity :: numeric, 10)
      ),
      4
    )
    and foi.asset_id = t.asset_id
),
cte2 AS (
  SELECT
    g.* ,
    concat_ws (
      '|',
      trunc(
        trim_scale(g.approved_units),
        4
      ),
      g.kristal_execution_account,
      jsonb_path_query(
        kristal_composition, '$.constituentAssets[*] ? (@.assetClass == "FUND")."assetId"'
      )
    ) AS goal_key,
    date(g.subscription_date) stime,
    date(g.approved_date) atime
  FROM
    funds_kristals.kristal_subscription_goal as g
    INNER JOIN kristaldata_kristals.kristal_properties as kp ON kp.kristal_id = g.kristal_id
)
SELECT
  (coalesce(txn.foi_goal_id,goal.kristal_subscription_goal_id))::text as goal_id, txn.* ,  goal.*
FROM
  cte1 AS txn
  LEFT OUTER JOIN cte2 as goal ON (
    txn.txn_key = goal.goal_key
    and (
      (
        txn.ttime = goal.stime
        or txn.ttime = goal.atime
      )
      or (
        txn.tctime = goal.stime
        or txn.tctime = goal.atime
      )
    )
  )
  and txn.foi_goal_id IS NULL  ;

WITH cte0 AS (
  SELECT
    a.asset_id as asset_id,
    ksg.kristal_execution_account as user_account_id,
    a.subscription_goal_id as subscription_goal_id,
    a.transaction_id as transaction_id,
    trunc(
      trim_scale(ksg.approved_units),
      4
    ) as approved_units
  from
    funds_orders.fund_order_info as a
    INNER JOIN funds_kristals.kristal_subscription_goal as ksg on ksg.kristal_subscription_goal_id = a.subscription_goal_id
    INNER JOIN kristaldata_kristals.kristal_properties as kp ON kp.kristal_id = ksg.kristal_id
    and a.asset_id = kp.lone_asset_id
),
cte1 AS (
  SELECT
    t.trade_type as trade_type,
    t.transaction_id as transaction_id,
    foi.subscription_goal_id AS foi_goal_id,
    concat_ws (
      '|',
      trunc(
        trim_scale(
          ROUND(t.quantity :: numeric, 10)
        ),
        4
      ),
      t.user_account_id,
      t.asset_id
    ) AS key,
    date(t.trade_time) ttime,
    date(t.created_time) tctime
  FROM
    funds_investo2o.transactions as t
    LEFT OUTER JOIN cte0 as foi ON t.user_account_id = foi.user_account_id
    and foi.transaction_id = t.external_transaction_id
    and foi.approved_units = trunc(
      trim_scale(
        ROUND(t.quantity :: numeric, 10)
      ),
      4
    )
    and foi.asset_id = t.asset_id
),
cte2 AS (
  SELECT
    g.kristal_subscription_goal_id as kristal_subscription_goal_id,
    concat_ws (
      '|',
      trunc(
        trim_scale(g.approved_units),
        4
      ),
      g.kristal_execution_account,
      jsonb_path_query(
        kristal_composition, '$.constituentAssets[*] ? (@.assetClass == "FUND")."assetId"'
      )
    ) AS key,
    date(g.subscription_date) stime,
    date(g.approved_date) atime
  FROM
    funds_kristals.kristal_subscription_goal as g
    INNER JOIN kristaldata_kristals.kristal_properties as kp ON kp.kristal_id = g.kristal_id
)
SELECT
  txn.trade_type as trade_type,
  txn.transaction_id AS transaction_id,
  txn.foi_goal_id AS foi_goal_id,
  goal.kristal_subscription_goal_id AS derived_goal_id,
  txn.key AS txn_key,
  goal.key AS goal_key
FROM
  cte1 AS txn
  LEFT OUTER JOIN cte2 as goal ON (
    txn.key = goal.key
    and (
      (
        txn.ttime = goal.stime
        or txn.ttime = goal.atime
      )
      or (
        txn.tctime = goal.stime
        or txn.tctime = goal.atime
      )
    )
  )
  and txn.foi_goal_id IS NULL
  where trade_type in ('BUY','SELL','ASSET_IN','ASSET_OUT') and txn.foi_goal_id  is null and goal.kristal_subscription_goal_id is null ;
  insert into orders.trades_tally (transaction_id,kristal_subscription_goal_id,kristal_subscription_id,user_id,user_account_id,quantity,approved_amount,asset_id, custom_asset_id,asset_type,trade_time,trade_type,trade_price,trade_nav,fee,tax,create_time,last_update_time,external_transaction_id,fee_currency,remarks,proposed_price,wm_fx_rate_to_base,base_currency,trade_purpose,original_trade_time,original_trade_price,original_trade_nav,original_transaction_id,accrued_interest,biz_notes,notes_updated_time,subscription_date,subscribed_by,approved_date,approved_by,source_type,audit_details,unit_price,cash_in_kristal_per_unit,total_cost,asset_wise_cost_map,execution_state,lifecycle_state,bookkeeping_state,unique_id,requested_units,requested_amount,original_request,bk_state_mover,fund_remarks,user_report_id,fund_bookkeeping,kristal_id,investment_rationale,temp_unit_price,temp_total_cost,approval_audit,platform,mechanism,activity_uuid,is_transfer,transaction_fees,original_subscription_date,original_unit_nav,original_investment_amount,expert_opinion_id,broker_price,client_price,execution_date,settlement_date,sn_note_size,spread,spread_amount,broker_settlement_amount,sn_net_subscription_amount,cost_with_fees,cost_without_fees,order_fees,limit_price,order_currency,dvp_route,shared_spread_amount,shared_spread_percentage,kristal_spread_amount,kristal_spread_percentage,kristal_access_fees,nav_date,payment_date,internal_cutoff,estimated_dates_audit,estimated_subscription_dates_id,estimated_redemption_dates_id) values ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}) ;
  select kristal_subscription_goal_id as goal_id from funds_kristals.kristal_subscription_goal ;




