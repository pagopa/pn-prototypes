-- SRS Monitoraggio Postalizzazione Work Item 9 --

/*
$QueryMetadata
{
    "name": "DroppedButNotCancelledReport",
    "dependencies": [
        {
            "name": "kpiSla",
            "location": "analog-delivery-monitoring/logical-views/kpiSla.sql"
        },
        {
            "name": "incremental_timeline",
            "location": "analog-delivery-monitoring/source-views/pnTimelines.sql"
        }
    ]
}
*/
WITH cancelledFromTimeline AS (
	SELECT t.*
	FROM incremental_timeline t
	WHERE t.category = 'NOTIFICATION_CANCELLED'
) SELECT 
	ks.ente_id,
	ks.requestID,
	ks.requestDateTime,
	ks.prodotto,
	ks.lotto,
	ks.recapitista,
	ks.geokey,
	ks.affido_consolidatore_data,
	ks.scarto_consolidatore_data,
	year(ks.affido_consolidatore_data) as year_affido_consolidatore,
	month(ks.affido_consolidatore_data) as month_affido_consolidatore
FROM kpisla ks 
LEFT JOIN cancelledFromTimeline t ON ks.iun = t.iun
WHERE t.iun IS NULL AND ks.scarto_consolidatore_stato = 'CON996';