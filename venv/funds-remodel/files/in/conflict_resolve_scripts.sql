select t.transaction_id , ksg.kristal_subscription_goal_id , t.trade_time, t.created_time as trade_create_time,
	ksg.create_time as goal_create_time , ksg.subscription_date , ksg.approved_date
from funds_investo2o.transactions t
inner join kristaldata_kristals.kristal_properties kp on kp.lone_asset_id = t.asset_id
left join funds_kristals.kristal_subscription_goal ksg on ksg.kristal_execution_account = t.user_account_id
		and trunc(trim_scale(ROUND(t.quantity :: numeric, 10)),4) = trunc(trim_scale(ksg.approved_units),4)
		and ksg.kristal_id = kp.kristal_id
		and t.asset_id = kp.lone_asset_id
where t.transaction_id in (132819,132820,132821,132823,132824,132825,159246,181416,182206,460266,460758,496496,496504,496521,496552,632537,632567,668201,668212,668235,668236,712066,712069,712072,712075,729240,742437,742438,743196,743200,744033,744038,752078,752079,753231,753236,753242,753243,753327,753330,755134,756183,756187,756190,756192,756195,762732,762733,765352,765404)
and ksg.kristal_subscription_goal_id is not null
order by t.transaction_id 	;

----------------------------------------------------------------------------------------------------------------------

Missing three in fund_tally
,,186334.0,45.0549|58602|1297648,,1297648.0,43557@inveeeeesto2o.com,,,
2081519.0,45.054|58602|1297648,,,22193.0,,43557@inveeeeesto2o.com,,,

,,172480.0,67.262|25400|1270123,,1270123.0,56102@inveeeeesto2o.com,DEFAULT,False,ACTIVE
2073219.0,145.156|25400|1270123,,,2971.0,,56102@inveeeeesto2o.com,DEFAULT,False,ACTIVE

2072221.0,61.1431|50474|1277694,,,3626.0,,,,,
,,182869.0,89.1238|50474|1277694,,1277694.0,,,,

INSERT INTO orders.fund_tally ( id, user_account_id, quantity, cost_nav, net_asset_value, dividends, eq_credit, eq_debit, transaction_fees, gain_or_loss, nav_calculation_time, created_time, updated_time, user_id, asset_id, custom_asset_id, return_percentage, fx_to_account, ib_leverage_in_account_currency, accrued_interest, kristal_id, no_of_subscribed_pending_units, amount_of_mf_order_pending, unit_cost_price, last_subscription_date, last_subscribed_by ) SELECT nextval('orders.fund_tally_pkey_seq'), f.user_account_id, f.quantity, f.cost_nav, f.net_asset_value, coalesce(f.dividends,0), f.eq_credit, f.eq_debit, f.transaction_fees, f.gain_or_loss, f.nav_calculation_time, Now(), Now(), f.user_id, f.asset_id, f.custom_asset_id, f.return_percentage, f.fx_to_account, f.ib_leverage_in_account_currency, f.accrued_interest, 22193 , 0,0, NULL , NULL, 0 FROM funds_investo2o.fund f WHERE f.id = 186334 on conflict do nothing  ;
INSERT INTO orders.fund_tally ( id, user_account_id, quantity, cost_nav, net_asset_value, dividends, eq_credit, eq_debit, transaction_fees, gain_or_loss, nav_calculation_time, created_time, updated_time, user_id, asset_id, custom_asset_id, return_percentage, fx_to_account, ib_leverage_in_account_currency, accrued_interest, kristal_id, no_of_subscribed_pending_units, amount_of_mf_order_pending, unit_cost_price, last_subscription_date, last_subscribed_by ) SELECT CASE WHEN s.kristal_subscription_id is not null THEN s.kristal_subscription_id ELSE nextval('orders.fund_tally_pkey_seq') END, s.kristal_execution_account, s.no_of_subscribed_approved_units, (coalesce(s.unit_cost_price,0) * s.no_of_subscribed_approved_units), (coalesce(s.unit_cost_price,0) * s.no_of_subscribed_approved_units), 0, 0, 0, 0, 0, null, Now(), Now(), s.user_id, 1297648 , null, 0, null, 0, 0, s.kristal_id, s.no_of_subscribed_pending_units, s.amount_of_mf_order_pending, s.unit_cost_price, s.last_subscription_date, s.last_subscribed_by FROM funds_kristals.kristal_subscription s where s.kristal_subscription_id = 2081519 on conflict do nothing  ;
INSERT INTO orders.fund_tally ( id, user_account_id, quantity, cost_nav, net_asset_value, dividends, eq_credit, eq_debit, transaction_fees, gain_or_loss, nav_calculation_time, created_time, updated_time, user_id, asset_id, custom_asset_id, return_percentage, fx_to_account, ib_leverage_in_account_currency, accrued_interest, kristal_id, no_of_subscribed_pending_units, amount_of_mf_order_pending, unit_cost_price, last_subscription_date, last_subscribed_by ) SELECT nextval('orders.fund_tally_pkey_seq'), f.user_account_id, f.quantity, f.cost_nav, f.net_asset_value, coalesce(f.dividends,0), f.eq_credit, f.eq_debit, f.transaction_fees, f.gain_or_loss, f.nav_calculation_time, Now(), Now(), f.user_id, f.asset_id, f.custom_asset_id, f.return_percentage, f.fx_to_account, f.ib_leverage_in_account_currency, f.accrued_interest, 2971 , 0,0, NULL , NULL, 0 FROM funds_investo2o.fund f WHERE f.id = 172480 on conflict do nothing  ;
INSERT INTO orders.fund_tally ( id, user_account_id, quantity, cost_nav, net_asset_value, dividends, eq_credit, eq_debit, transaction_fees, gain_or_loss, nav_calculation_time, created_time, updated_time, user_id, asset_id, custom_asset_id, return_percentage, fx_to_account, ib_leverage_in_account_currency, accrued_interest, kristal_id, no_of_subscribed_pending_units, amount_of_mf_order_pending, unit_cost_price, last_subscription_date, last_subscribed_by ) SELECT CASE WHEN s.kristal_subscription_id is not null THEN s.kristal_subscription_id ELSE nextval('orders.fund_tally_pkey_seq') END, s.kristal_execution_account, s.no_of_subscribed_approved_units, (coalesce(s.unit_cost_price,0) * s.no_of_subscribed_approved_units), (coalesce(s.unit_cost_price,0) * s.no_of_subscribed_approved_units), 0, 0, 0, 0, 0, null, Now(), Now(), s.user_id, 1277694 , null, 0, null, 0, 0, s.kristal_id, s.no_of_subscribed_pending_units, s.amount_of_mf_order_pending, s.unit_cost_price, s.last_subscription_date, s.last_subscribed_by FROM funds_kristals.kristal_subscription s where s.kristal_subscription_id = 2072221    ;
---------------------------------------------------------------------------------------------------------------------------------------

Erred out in trades_tally because asset_id is null
select lifecycle_state from funds_kristals.kristal_subscription_goal where kristal_subscription_goal_id in (2285176 , 2285175, 191298, 191297) ;
select lone_asset_id , kristal_composition from kristaldata_kristals.kristal_properties where kristal_id in (6508,13278) ;
select * from funds_kristals.kristal_subscription where kristal_subscription_id in (2068230,2068229,49982) ;
