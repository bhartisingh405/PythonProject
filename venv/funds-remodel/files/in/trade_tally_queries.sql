select count(*) from funds_investo2o.transactions ;
select kristal_subscription_goal_id as goal_id , kristal_id as kristal_id from funds_kristals.kristal_subscription_goal ;
WITH cte0 AS (
  SELECT
    a.asset_id as asset_id,
    ksg.kristal_execution_account as user_account_id,
    a.subscription_goal_id as subscription_goal_id,
    a.transaction_id as transaction_id,
    trunc(
      trim_scale(ksg.approved_units),
      4
    ) as approved_units,
    ksg.kristal_id as kristal_id
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
    date(t.created_time) tctime,
    t.asset_id as a_id,
    foi.kristal_id as kristal_id
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
    where g.lifecycle_state = 'APPROVED'
)
SELECT
  (coalesce(txn.foi_goal_id,goal.kristal_subscription_goal_id))::text as goal_id,
  txn.transaction_id as transaction_id,
  txn.trade_type as trade_type,
  goal.kristal_subscription_goal_id as kristal_subscription_goal_id,
  txn.foi_goal_id AS foi_goal_id,
  coalesce(goal.kristal_id,txn.kristal_id) as kristal_id,
  txn.asset_id as asset_id
FROM
  cte1 AS txn
  LEFT JOIN cte2 as goal ON (
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
Insert into orders.trades_tally (goal_id,user_id,user_account_id,quantity,approved_amount,asset_id, custom_asset_id,asset_type,trade_time,trade_type,trade_price,trade_nav,fee,tax,create_time,last_update_time,external_transaction_id,fee_currency,remarks,proposed_price,wm_fx_rate_to_base,base_currency,trade_purpose,original_trade_time,original_trade_price,original_trade_nav,original_transaction_id,accrued_interest,biz_notes,notes_updated_time,subscription_date,subscribed_by,approved_date,approved_by,source_type,audit_details,unit_price,cash_in_kristal_per_unit,total_cost,asset_wise_cost_map,execution_state,lifecycle_state,bookkeeping_state,unique_id,requested_units,requested_amount,original_request,bk_state_mover,fund_remarks,user_report_id,fund_bookkeeping,kristal_id,investment_rationale,temp_unit_price,temp_total_cost,approval_audit,platform,mechanism,activity_uuid,is_transfer,transaction_fees,original_subscription_date,original_unit_nav,original_investment_amount,expert_opinion_id,broker_price,client_price,execution_date,settlement_date,sn_note_size,spread,spread_amount,broker_settlement_amount,sn_net_subscription_amount,cost_with_fees,cost_without_fees,order_fees,limit_price,order_currency,dvp_route,shared_spread_amount,shared_spread_percentage,kristal_spread_amount,kristal_spread_percentage,kristal_access_fees,nav_date,payment_date,internal_cutoff,estimated_dates_audit,estimated_subscription_dates_id,estimated_redemption_dates_id) values ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}) ;
INSERT INTO orders.trades_tally (id, user_id, user_account_id, quantity, approved_amount, asset_id, custom_asset_id, asset_type, trade_time, trade_type, trade_price, trade_nav, fee, tax, create_time, last_update_time, external_transaction_id, fee_currency, remarks, proposed_price, wm_fx_rate_to_base, base_currency, trade_purpose, original_trade_time, original_trade_price, original_trade_nav, original_transaction_id, accrued_interest, biz_notes, notes_updated_time, subscription_date, subscribed_by, approved_date, approved_by, source_type, audit_details, unit_price, cash_in_kristal_per_unit, total_cost, asset_wise_cost_map, execution_state, lifecycle_state, bookkeeping_state, unique_id, requested_units, requested_amount, original_request, bk_state_mover, fund_remarks, user_report_id, fund_bookkeeping, kristal_id, investment_rationale, temp_unit_price, temp_total_cost, approval_audit, platform, mechanism, activity_uuid, is_transfer, transaction_fees, original_subscription_date, original_unit_nav, original_investment_amount, expert_opinion_id, broker_price, client_price, execution_date, settlement_date, sn_note_size, spread, spread_amount, broker_settlement_amount, sn_net_subscription_amount, cost_with_fees, cost_without_fees, order_fees, limit_price, order_currency, dvp_route, shared_spread_amount, shared_spread_percentage, kristal_spread_amount, kristal_spread_percentage, kristal_access_fees, nav_date, payment_date, internal_cutoff, estimated_dates_audit, estimated_subscription_dates_id, estimated_redemption_dates_id,fund_tally_id) SELECT nextval('orders.trades_tally_pkey_seq'), t.user_id, t.user_account_id, t.quantity, 0, t.asset_id, t.custom_asset_id, t.asset_type, t.trade_time, t.trade_type, t.trade_price, t.trade_nav, coalesce(t.fees,0), coalesce(t.taxes,0), t.created_time, t.last_updated_time, t.external_transaction_id, t.fee_currency, t.remarks, t.proposed_price, t.wm_fx_rate_to_base, t.base_currency, t.trade_purpose, t.original_trade_time, t.original_trade_price, t.original_trade_nav, t.original_transaction_id, coalesce(t.accrued_interest,0), t.biz_notes, t.notes_updated_time, NULL, NULL, NULL, NULL, NULL, '{{}}'::jsonb, 0, 0, NULL, '{{}}'::jsonb, 'COMPLETED', 'APPROVED', 'FILLED', NULL, 0, 0, '{{}}'::jsonb, NULL, NULL, NULL, NULL, {} , NULL, NULL, NULL, NULL, 'NA', 'NA', NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{{}}'::jsonb, NULL, NULL, false, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (select fd.id from orders.fund_tally as fd where fd.user_account_id = t.user_account_id and fd.asset_id = t.asset_id) FROM investo2o.transactions t where t.transaction_id = {} on conflict do nothing ;
INSERT INTO orders.trades_tally (id, user_id, user_account_id, quantity, approved_amount, asset_id, custom_asset_id, asset_type, trade_time, trade_type, trade_price, trade_nav, fee, tax, create_time, last_update_time, external_transaction_id, fee_currency, remarks, proposed_price, wm_fx_rate_to_base, base_currency, trade_purpose, original_trade_time, original_trade_price, original_trade_nav, original_transaction_id, accrued_interest, biz_notes, notes_updated_time, subscription_date, subscribed_by, approved_date, approved_by, source_type, audit_details, unit_price, cash_in_kristal_per_unit, total_cost, asset_wise_cost_map, execution_state, lifecycle_state, bookkeeping_state, unique_id, requested_units, requested_amount, original_request, bk_state_mover, fund_remarks, user_report_id, fund_bookkeeping, kristal_id, investment_rationale, temp_unit_price, temp_total_cost, approval_audit, platform, mechanism, activity_uuid, is_transfer, transaction_fees, original_subscription_date, original_unit_nav, original_investment_amount, expert_opinion_id, broker_price, client_price, execution_date, settlement_date, sn_note_size, spread, spread_amount, broker_settlement_amount, sn_net_subscription_amount, cost_with_fees, cost_without_fees, order_fees, limit_price, order_currency, dvp_route, shared_spread_amount, shared_spread_percentage, kristal_spread_amount, kristal_spread_percentage, kristal_access_fees, nav_date, payment_date, internal_cutoff, estimated_dates_audit, estimated_subscription_dates_id, estimated_redemption_dates_id,fund_tally_id) SELECT CASE WHEN g.kristal_subscription_goal_id is not null THEN g.kristal_subscription_goal_id ELSE nextval('orders.trades_tally_pkey_seq') END, g.user_id, g.kristal_execution_account, g.approved_units, g.approved_amount, {} , NULL, 'FUND', g.last_update_time, 'NA', 0, 0, coalesce(g.fee,0), coalesce(g.tax,0), g.create_time, g.last_update_time, NULL, NULL, NULL, NULL, NULL, NULL, 'DEFAULT', NULL, NULL, NULL, NULL, 0, NULL, NULL, g.subscription_date, g.subscribed_by, g.approved_date, g.approved_by, g.source_type, coalesce(g.audit_details,'{{}}'::jsonb), g.unit_price, coalesce(g.cash_in_kristal_per_unit,0), g.total_cost, coalesce(g.asset_wise_cost_map,'{{}}'::jsonb), g.subscription_pending_execution_state, g.lifecycle_state, g.bookkeeping_state, g.unique_id, g.requested_units, g.requested_amount, coalesce(g.original_request,'{{}}'::jsonb), g.bk_state_mover, g.fund_remarks, g.user_report_id, g.fund_bookkeeping, g.kristal_id, g.investment_rationale, g.temp_unit_price, g.temp_total_cost, g.approval_audit, g.platform, g.mechanism, g.activity_uuid, g.is_transfer, coalesce(g.transaction_fees,0), g.original_subscription_date, g.original_unit_nav, g.original_investment_amount, g.expert_opinion_id, g.broker_price, g.client_price, g.execution_date, g.settlement_date, g.sn_note_size, g.spread, g.spread_amount, g.broker_settlement_amount, g.sn_net_subscription_amount, g.cost_with_fees, g.cost_without_fees, coalesce(g.order_fees,'{{}}'::jsonb), g.limit_price, g.order_currency, g.dvp_route, g.shared_spread_amount, g.shared_spread_percentage, g.kristal_spread_amount, g.kristal_spread_percentage, g.kristal_access_fees, g.nav_date, g.payment_date, g.internal_cutoff, g.estimated_dates_audit, g.estimated_subscription_dates_id, g.estimated_redemption_dates_id, g.kristal_subscription_id FROM kristals.kristal_subscription_goal g where g.kristal_subscription_goal_id = {} on conflict do nothing ;
INSERT INTO orders.trades_tally (id, user_id, user_account_id, quantity, approved_amount, asset_id, custom_asset_id, asset_type, trade_time, trade_type, trade_price, trade_nav, fee, tax, create_time, last_update_time, external_transaction_id, fee_currency, remarks, proposed_price, wm_fx_rate_to_base, base_currency, trade_purpose, original_trade_time, original_trade_price, original_trade_nav, original_transaction_id, accrued_interest, biz_notes, notes_updated_time, subscription_date, subscribed_by, approved_date, approved_by, source_type, audit_details, unit_price, cash_in_kristal_per_unit, total_cost, asset_wise_cost_map, execution_state, lifecycle_state, bookkeeping_state, unique_id, requested_units, requested_amount, original_request, bk_state_mover, fund_remarks, user_report_id, fund_bookkeeping, kristal_id, investment_rationale, temp_unit_price, temp_total_cost, approval_audit, platform, mechanism, activity_uuid, is_transfer, transaction_fees, original_subscription_date, original_unit_nav, original_investment_amount, expert_opinion_id, broker_price, client_price, execution_date, settlement_date, sn_note_size, spread, spread_amount, broker_settlement_amount, sn_net_subscription_amount, cost_with_fees, cost_without_fees, order_fees, limit_price, order_currency, dvp_route, shared_spread_amount, shared_spread_percentage, kristal_spread_amount, kristal_spread_percentage, kristal_access_fees, nav_date, payment_date, internal_cutoff, estimated_dates_audit, estimated_subscription_dates_id, estimated_redemption_dates_id,fund_tally_id) SELECT CASE WHEN g.kristal_subscription_goal_id is not null THEN g.kristal_subscription_goal_id ELSE nextval('orders.trades_tally_pkey_seq') END, t.user_id, t.user_account_id, t.quantity, g.approved_amount, t.asset_id, t.custom_asset_id, t.asset_type, t.trade_time, t.trade_type, t.trade_price, t.trade_nav, coalesce(t.fees,0), coalesce(t.taxes,0), t.created_time, t.last_updated_time, t.external_transaction_id, t.fee_currency, t.remarks, t.proposed_price, t.wm_fx_rate_to_base, t.base_currency, t.trade_purpose, t.original_trade_time, t.original_trade_price, t.original_trade_nav, t.original_transaction_id, t.accrued_interest, t.biz_notes, t.notes_updated_time, g.subscription_date, g.subscribed_by, g.approved_date, g.approved_by, g.source_type, coalesce(g.audit_details,'{{}}'::jsonb), g.unit_price, coalesce(g.cash_in_kristal_per_unit,0), g.total_cost, coalesce(g.asset_wise_cost_map,'{{}}'::jsonb), g.subscription_pending_execution_state, g.lifecycle_state, g.bookkeeping_state, g.unique_id, g.requested_units, g.requested_amount, coalesce(g.original_request,'{{}}'::jsonb), g.bk_state_mover, g.fund_remarks, g.user_report_id, g.fund_bookkeeping, g.kristal_id, g.investment_rationale, g.temp_unit_price, g.temp_total_cost, g.approval_audit, g.platform, g.mechanism, g.activity_uuid, g.is_transfer, coalesce(g.transaction_fees,0), g.original_subscription_date, g.original_unit_nav, g.original_investment_amount, g.expert_opinion_id, g.broker_price, g.client_price, g.execution_date, g.settlement_date, g.sn_note_size, g.spread, g.spread_amount, g.broker_settlement_amount, g.sn_net_subscription_amount, g.cost_with_fees, g.cost_without_fees, coalesce(g.order_fees,'{{}}'::jsonb), g.limit_price, g.order_currency, g.dvp_route, g.shared_spread_amount, g.shared_spread_percentage, g.kristal_spread_amount, g.kristal_spread_percentage, g.kristal_access_fees, g.nav_date, g.payment_date, g.internal_cutoff, g.estimated_dates_audit, g.estimated_subscription_dates_id, g.estimated_redemption_dates_id, g.kristal_subscription_id FROM investo2o.transactions t , kristals.kristal_subscription_goal g where g.kristal_subscription_goal_id = {} and t.transaction_id = {} on conflict do nothing ;
select kristal_subscription_goal_id as goal_id , kristal_id as kristal_id from funds_kristals.kristal_subscription_goal  where lifecycle_state != 'APPROVED' ;