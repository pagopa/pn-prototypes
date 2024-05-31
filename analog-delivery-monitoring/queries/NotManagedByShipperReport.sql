-- SRS Monitoraggio Postalizzazione Work Item 8 --

/*
$QueryMetadata
{
    "name": "NotManagedByShipperReport",
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
	ks.requestDateTime,
	ks.prodotto,
	ks.geokey,
	ks.recapitista,
	ks.lotto,
	ks.codiceOggetto,
	ks.affido_consolidatore_data,
	ks.stampa_imbustamento_CON080_data,
	ks.affido_recapitista_CON016_data,
	ks.accettazione_recapitista_CON018_data,
	ks.scarto_consolidatore_stato,
	ks.scarto_consolidatore_data,
	ks.tentativo_recapito_stato,
	ks.tentativo_recapito_data,
	ks.tentativo_recapito_data_rendicontazione,
	ks.messaingiacenza_recapito_stato,
	ks.messaingiacenza_recapito_data,
	ks.messaingiacenza_recapito_data_rendicontazione,
	ks.certificazione_recapito_stato,
	ks.certificazione_recapito_dettagli,
	ks.certificazione_recapito_data,
	ks.certificazione_recapito_data_rendicontazione,
	ks.fine_recapito_stato,
	ks.fine_recapito_data,
	ks.fine_recapito_data_rendicontazione,
	ks.accettazione_23L_RECAG012_data,
	ks.accettazione_23L_RECAG012_data_rendicontazione,
	ks.demat_23L_stato,
	ks.demat_23L_data,
	ks.demat_23L_data_rendicontazione,
	year(ks.affido_recapitista_CON016_data) as year_affido_recapitista,
	month(ks.affido_recapitista_CON016_data) as month_affido_recapitista
FROM kpiSla ks
WHERE 
	datediff(current_date(), ks.affido_recapitista_CON016_data) > 14 AND
	ks.tentativo_recapito_stato IS NULL AND
	ks.messaingiacenza_recapito_stato IS NULL AND
	ks.certificazione_recapito_stato IS NULL AND
	ks.fine_recapito_stato IS NULL AND
	ks.demat_23L_stato IS NULL
;