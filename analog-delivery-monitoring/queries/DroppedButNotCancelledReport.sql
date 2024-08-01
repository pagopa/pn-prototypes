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
), reportsla_with_requestId AS (
    SELECT
        r.*, REGEXP_REPLACE(r.requestID, '(\\.)?PCRETRY_[0-9]+', '') as requestIdWithoutPcRetry
    from kpiSla r
), reportsla_ranked_by_requestId AS (
    SELECT
        r.*,
        row_number() over (partition by r.requestIdWithoutPcRetry order by r.requestID desc) as row_number_rank
    from reportsla_with_requestId r
) , reportSlaLastPcretry AS (
    SELECT *
    FROM reportsla_ranked_by_requestId r
    WHERE r.row_number_rank = 1
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
FROM reportSlaLastPcretry ks
    LEFT JOIN cancelledFromTimeline t ON ks.iun = t.iun
WHERE t.iun IS NULL AND ks.scarto_consolidatore_stato = 'CON996';