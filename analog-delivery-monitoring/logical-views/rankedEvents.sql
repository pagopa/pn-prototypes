/*
$QueryMetadata
{
    "name": "rankedEvents",
    "dependencies": [
        {
            "name": "completeUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/completeUpdatedEcMetadata.sql"
        }
    ]
}
*/
create or replace temporary view rankedEvents as
	WITH allEvents AS (
		SELECT
			paper_request_id AS requestId,
			costi_recapito.recapitista AS shipper,
			costi_recapito.lotto AS lot,
			costi_recapito.geokey AS geokey,
			ec_metadata.paperMeta_productType as product,
			e.paperProg_status as status,
			e.paperProg_statusDateTime as statusDateTime
		FROM complete_updated_ec_metadata c LATERAL VIEW EXPLODE(c.ec_metadata.event_list) as e
	), allEventsRanked AS (
		SELECT DISTINCT
			e.requestId,
			e.shipper,
			e.lot,
			e.geokey,
			e.product,
			e.status,
			e.statusDateTime,
			dense_rank() over(PARTITION BY e.requestId order by e.statusDateTime, e.status) as ranking
		FROM allEvents e
	) SELECT * FROM allEventsRanked
;