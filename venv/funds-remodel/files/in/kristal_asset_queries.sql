select kristal_id as kristal_id, coalesce(jsonb_path_query_first(
                            kristal_composition, '$.constituentAssets[*] ? (@.assetClass == "FUND")."assetId"'
                          )::bigint , lone_asset_id) as asset_id
from kristaldata_kristals.kristal_properties ;
select coalesce(jsonb_path_query_first(
                            kristal_composition, '$.constituentAssets[*] ? (@.assetClass == "FUND")."assetId"'
                        )::bigint , lone_asset_id) as asset_id , kristal_id as kristal_id
from kristaldata_kristals.kristal_properties ;

