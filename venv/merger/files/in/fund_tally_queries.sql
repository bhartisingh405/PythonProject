select count(*) from funds_kristals.kristal_subscription as ks where ks.no_of_subscribed_approved_units > 0.0 and exists (select ksg.kristal_subscription_goal_id from funds_kristals.kristal_subscription_goal as ksg where ksg.kristal_subscription_id = ks.kristal_subscription_id limit 1 );
select count(*) from funds_investo2o.fund where quantity > 0.0 ;
WITH
  cte0 AS (

    SELECT ks.kristal_subscription_id as kristal_subscription_id,
    concat_ws (
        '|',
        trim_scale(ROUND(ks.no_of_subscribed_approved_units::numeric, 10)),
        ks.kristal_execution_account,
        kp.lone_asset_id
      ) as skey,
	u.email as user_email,
	ua.compliance_mode as compliance_mode,
	ua.is_model_portfolio as is_model_portfolio,
	ua.user_account_status as user_account_status
    from funds_kristals.kristal_subscription as ks
        INNER JOIN kristaldata_kristals.kristal_properties as kp ON kp.kristal_id = ks.kristal_id
		LEFT JOIN vpn2_investo2o.users as u ON u.user_id = ks.user_id
	    LEFT JOIN vpn2_investo2o.user_accounts as ua ON ua.user_account_id = ks.kristal_execution_account
    where exists (select ksg.kristal_subscription_goal_id from funds_kristals.kristal_subscription_goal as ksg
						where ksg.kristal_subscription_id = ks.kristal_subscription_id limit 1 )
		and ks.no_of_subscribed_approved_units > 0.0
  ),
  cte1 AS (
    SELECT
      id as fund_id ,
      concat_ws (
        '|',
        trim_scale(ROUND(quantity::numeric, 10)),
        fund.user_account_id,
        asset_id
      ) as fkey,
	u.email as user_email,
	ua.compliance_mode as compliance_mode,
	ua.is_model_portfolio as is_model_portfolio,
	ua.user_account_status as user_account_status
    FROM funds_investo2o.fund  as fund
	LEFT JOIN vpn2_investo2o.users as u ON u.user_id = fund.user_id
	LEFT JOIN vpn2_investo2o.user_accounts as ua ON ua.user_account_id = fund.user_account_id
	where quantity > 0.0
  ) SELECT ksub.kristal_subscription_id as kristal_subscription_id ,
    ksub.skey as skey ,
    fd.fund_id as fund_id ,
    fd.fkey as fkey,
	coalesce(fd.user_email,ksub.user_email) as email,
	coalesce(fd.compliance_mode,ksub.compliance_mode) as compliance_mode,
	coalesce(fd.is_model_portfolio,ksub.is_model_portfolio) as is_model_account,
	coalesce(fd.user_account_status,ksub.user_account_status) as account_status
   FROM cte0 as ksub FULL JOIN cte1 as fd ON fd.fkey = ksub.skey ;
  insert into orders.fund_tally (user_account_id,quantity,cost_nav,net_asset_value,dividends,eq_credit,eq_debit, transaction_fees,gain_or_loss,nav_calculation_time,created_time,updated_time,user_id,asset_id,custom_asset_id,return_percentage,fx_to_account,ib_leverage_in_account_currency,accrued_interest,kristal_id,no_of_subscribed_pending_units,amount_of_mf_order_pending,unit_cost_price,last_subscription_date,last_subscribed_by) values ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}) ;
