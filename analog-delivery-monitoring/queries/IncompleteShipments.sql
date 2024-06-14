/*
$QueryMetadata
{
    "name": "IncompleteShipments",
    "dependencies": [
        {
            "name": "kpiSla",
            "location": "analog-delivery-monitoring/logical-views/kpiSla.sql"
        }
    ]
}
*/
WITH NonCompletate AS (
	SELECT *
	FROM kpiSla ks
	WHERE ks.fine_recapito_stato IS NULL
		AND ks.ente_id != '4a4149af-172e-4950-9cc8-63ccc9a6d865'
), TentativoRecapito AS (
	SELECT
		nc.*,
		'Tentativo recapito' AS cluster,
		1 AS priority
	FROM NonCompletate nc
	WHERE nc.tentativo_recapito_stato IS NULL
		AND datediff(current_date(), nc.affido_recapitista_CON016_data) >= 14
), ConsegnaMancataConsegna AS (
	SELECT
		nc.*,
		'Consegna-Mancata consegna' AS cluster,
		2 AS priority
	FROM NonCompletate nc
	WHERE nc.certificazione_recapito_stato LIKE 'REC%A'
		AND datediff(current_date(), nc.certificazione_recapito_data) >=
		    CASE
                WHEN nc.prodotto = '890' OR nc.prodotto = 'AR'
                THEN 7
                ELSE 15
            END
), Irreperibilita AS (
	SELECT
		nc.*,
		'Irreperibilità' AS cluster,
		3 AS priority
	FROM NonCompletate nc
	WHERE nc.certificazione_recapito_stato LIKE 'REC%D'
		AND datediff(current_date(), nc.certificazione_recapito_data) >= 21
), MancanzaRECAG012 AS (
	SELECT
		nc.*,
		'Mancanza RECAG012' AS cluster,
		4 AS priority
	FROM NonCompletate nc
	WHERE nc.tentativo_recapito_stato = 'RECAG010'
		AND nc.accettazione_23L_RECAG012_data IS NULL
		AND datediff(current_date(), nc.tentativo_recapito_data) >= 18
), MancanzaDemat23L AS (
	SELECT
		nc.*,
		'Mancanza Demat 23L' AS cluster,
		5 AS priority
	FROM NonCompletate nc
	WHERE nc.demat_23L_data_rendicontazione IS NULL
		AND datediff(current_date(), nc.accettazione_23L_RECAG012_data) >= 7
), CompiutaGiacenza AS (
	SELECT
		nc.*,
		'Compiuta giacenza' AS cluster,
		6 AS priority
	FROM NonCompletate nc
	WHERE nc.demat_23L_data_rendicontazione IS NOT NULL
		AND nc.accettazione_23L_RECAG012_data IS NOT NULL
		AND datediff(current_date(), nc.tentativo_recapito_data) >= 200
), GiacenzaRS_AR AS (
	SELECT
		nc.*,
		'Giacenza RS_AR' AS cluster,
		7 AS priority
	FROM NonCompletate nc
	WHERE (nc.tentativo_recapito_stato = 'RECRN010'
			OR nc.tentativo_recapito_stato = 'RECRS010')
		AND datediff(current_date(), nc.tentativo_recapito_data) >= 51
),
AllData AS (
	SELECT * FROM TentativoRecapito
	UNION ALL
	SELECT * FROM ConsegnaMancataConsegna
	UNION ALL
	SELECT * FROM Irreperibilita
	UNION ALL
	SELECT * FROM MancanzaRECAG012
	UNION ALL
	SELECT * FROM MancanzaDemat23L
	UNION ALL
	SELECT * FROM CompiutaGiacenza
	UNION ALL
	SELECT * FROM GiacenzaRS_AR
),
RankedData AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY ad.requestID ORDER BY ad.priority) AS rank_number
	FROM AllData ad
),
RankedFiltered AS (
	SELECT *
	FROM RankedData rd
	WHERE rd.rank_number = 1
),
ConRecapitista AS (
	SELECT 
		*,
		CASE
	        WHEN (rf.recapitista LIKE '%Poste%') OR (rf.recapitista LIKE 'FSU%')
	        	THEN 'Poste'
	        WHEN rf.recapitista LIKE 'RTI Fulmine%'
	        	THEN 'Fulmine'
	        ELSE rf.recapitista
		END AS recapitistaUnified
	FROM RankedFiltered rf
)
SELECT 
	iun,
	requestID,
	requestDateTime,
	prodotto,
	geokey,
	recapitista,
	lotto,
	codiceOggetto,
	affido_recapitista_CON016_data,
	accettazione_recapitista_CON018_data,
	scarto_consolidatore_stato,scarto_consolidatore_data,
	tentativo_recapito_stato,
	tentativo_recapito_data,
	tentativo_recapito_data_rendicontazione,
	messaingiacenza_recapito_stato,
	messaingiacenza_recapito_data,
	messaingiacenza_recapito_data_rendicontazione,
	certificazione_recapito_stato,
	certificazione_recapito_dettagli,
	certificazione_recapito_data,
	certificazione_recapito_data_rendicontazione,
	fine_recapito_stato,
	fine_recapito_data,
	fine_recapito_data_rendicontazione,
	accettazione_23L_RECAG012_data,
	accettazione_23L_RECAG012_data_rendicontazione,
	demat_23L_stato,
	demat_23L_data,
	demat_23L_data_rendicontazione,
	recapitistaUnified,
	cluster
FROM ConRecapitista
ORDER BY requestDateTime;