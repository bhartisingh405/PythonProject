select transaction_id as txn_id from funds_investo2o.transactions ;
select transaction_id as txn_id from funds_investo2o.transactions where trade_type in ('BUY','SELL','ASSET_IN','ASSET_OUT') ;
select kristal_subscription_goal_id as goal_id from funds_kristals.kristal_subscription_goal ;
select kristal_subscription_goal_id as goal_id from funds_kristals.kristal_subscription_goal where  lifecycle_state = 'APPROVED' ;