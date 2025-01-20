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

