-- SRS Monitoraggio Postalizzazione Work Item 6 --

/*
$QueryMetadata
{
    "name": "MissingDemat23LReport",
    "dependencies": [
        {
            "name": "kpiSla",
            "location": "analog-delivery-monitoring/logical-views/kpiSla.sql"
        }
    ]
}
*/
SELECT
	ks.iun,
	ks.requestID,
	ks.recapitista,
	ks.codiceOggetto,
	ks.certificazione_recapito_stato,
	ks.certificazione_recapito_data,
	ks.fine_recapito_stato,
	ks.fine_recapito_data,
	ks.accettazione_23L_RECAG012_data,
	year(ks.affido_consolidatore_data) as year_affido_consolidatore,
	month(ks.affido_consolidatore_data) as month_affido_consolidatore
FROM kpiSla ks
WHERE 
	(ks.certificazione_recapito_stato IN ('RECAG001A', 'RECAG002A', 'RECAG005A', 'RECAG006A', 'RECAG007A') OR accettazione_23L_RECAG012_data IS NOT NULL) AND
	datediff(current_date(), COALESCE(ks.certificazione_recapito_data, ks.accettazione_23L_RECAG012_data)) > 7 AND
	ks.demat_23L_stato IS NULL
ORDER BY ks.certificazione_recapito_data ASC, ks.accettazione_23L_RECAG012_data ASC