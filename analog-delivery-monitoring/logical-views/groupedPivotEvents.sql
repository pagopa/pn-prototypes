/*
$QueryMetadata
{
    "name": "groupedPivotEvents",
    "dependencies": [
    	{
            "name": "rankedEvents",
            "location": "analog-delivery-monitoring/logical-views/rankedEvents.sql"
        },
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/logical-views/pivotedEvents.sql"
        }
    ]
}
*/
create or replace temporary view groupedPivotEvents as
	WITH lastEvents AS (
		SELECT e.requestId, e.shipper, e.product, e.lot, e.geokey, e.status, max(e.ranking) as maxRanking
		FROM rankedEvents e
		WHERE e.status RLIKE("(CON016)|(REC.*)")
		GROUP BY e.requestId, e.shipper, e.product, e.status, e.lot, e.geokey
	), joinWithPivot AS (
		SELECT pe.*, pe.originalStatus AS status
		FROM lastEvents le 
		JOIN pivotedEvents pe
		ON le.requestId = pe.requestId AND le.shipper = pe.shipper AND le.product = pe.product AND le.status = pe.originalStatus AND le.maxRanking = pe.ranking
	) SELECT * FROM joinWithPivot;
;