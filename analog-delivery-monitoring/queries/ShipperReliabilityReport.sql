-- [MOSPE] PN-10654 --

/*
$QueryMetadata
{
    "name": "events",
    "dependencies": [
        {
            "name": "completeUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/completeUpdatedEcMetadata.sql"
        }
    ]
}
*/
create or replace temporary view events as
	WITH all_events AS (
		SELECT
			paper_request_id AS requestId,
			costi_recapito.recapitista AS shipper,
			ec_metadata.paperMeta_productType as product,
			e.paperProg_status as status,
			e.paperProg_statusDateTime as statusDateTime
		FROM complete_updated_ec_metadata c LATERAL VIEW EXPLODE(c.ec_metadata.event_list) as e
		WHERE ec_metadata.paperMeta_productType IN ('890', 'AR', 'RS')
	), all_events_ranked AS (
		SELECT DISTINCT requestId, shipper, product, status, max(statusDateTime) over(PARTITION BY requestId, status)
		FROM all_events e
		WHERE e.status RLIKE("(CON016)|(REC.*)")
	), events_grouped AS (
		SELECT r.shipper, r.product, r.status, COUNT(*) AS _count
		FROM all_events_ranked r
		GROUP BY r.shipper, r.product, r.status
	), events_pivoted AS (
		SELECT * FROM events_grouped
		PIVOT
		(
			first(_count)
			FOR status IN (
				'CON016',
				'RECRN001A', 'RECRN002A', 'RECRN003A', 'RECRN004A', 'RECRN005A', 'RECRN001C', 'RECRN002C', 'RECRN003C', 'RECRN004C', 'RECRN005C', 'RECRN002D', 'RECRN002F', 'RECRN006', 'RECRN010', 'RECRN011', 'RECRN013',
				'RECAG001A', 'RECAG002A', 'RECAG003A', 'RECAG005A', 'RECAG006A', 'RECAG007A', 'RECAG008A', 'RECAG001C', 'RECAG002C', 'RECAG003C', 'RECAG005C', 'RECAG006C', 'RECAG007C', 'RECAG008C', 'RECAG003D', 'RECAG003F', 'RECAG004', 'RECAG010', 'RECAG011A', 'RECAG013',
				'RECRS002A', 'RECRS004A', 'RECRS005A', 'RECRS001C', 'RECRS002C', 'RECRS003C', 'RECRS004C', 'RECRS005C', 'RECRS002D', 'RECRS002F', 'RECRS006', 'RECRS010', 'RECRS011', 'RECRS013'
			)
		)
	) SELECT * FROM events_pivoted
;

/*
$QueryMetadata
{
    "name": "product890",
    "dependencies": [
        {
            "name": "events",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW product890 AS
	WITH product890PartialKpis AS (
		SELECT
			e.shipper,
			e.product,
			COALESCE(e.CON016, 0) AS PreseInCarico,
			COALESCE(e.RECAG001A, 0) + COALESCE(e.RECAG002A, 0) AS Consegnate,
			(COALESCE(e.RECAG001C, 0) + COALESCE(e.RECAG002C, 0)) / (COALESCE(e.RECAG001A, 0) + COALESCE(e.RECAG002A, 0)) AS PercentualeDematConsegnate,
			COALESCE(e.RECAG003A, 0) AS MancatoRecapito,
			COALESCE(e.RECAG003C, 0) / COALESCE(e.RECAG003A, 0) AS PercentualeDematMancatoRecapito,
			COALESCE(e.RECAG003D, 0) AS Irreperibile,
			COALESCE(e.RECAG003F, 0) / COALESCE(e.RECAG003D, 0) AS PercentualeDematIrreperibile,
			COALESCE(e.RECAG010, 0) AS Inesito,
			COALESCE(e.RECAG011A, 0) AS InGiacenza,
			COALESCE(e.RECAG005A, 0) + COALESCE(e.RECAG005A, 0) AS ConsegnateInGiacenza,
			(COALESCE(e.RECAG005C, 0) + COALESCE(e.RECAG006C, 0)) / (COALESCE(e.RECAG005A, 0) + COALESCE(e.RECAG006A, 0)) AS PercentualeDematConsegnateInGiacenza,
			COALESCE(e.RECAG007A, 0) AS MancataConsegnaInGiacenza,
			COALESCE(e.RECAG007C, 0) / COALESCE(e.RECAG007A, 0) AS PercentualeDematMancataConsegnaInGiacenza,
			COALESCE(e.RECAG008A, 0) AS CompiutaGiacenza,
			COALESCE(e.RECAG008C, 0) / COALESCE(e.RECAG008A, 0) AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECAG004, 0) + COALESCE(e.RECAG013, 0) AS NonRendicontabili
		FROM events e
		WHERE e.product = "890"
	), product890Kpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM product890PartialKpis p
	) SELECT * FROM product890Kpis
;

/*
$QueryMetadata
{
    "name": "productAR",
    "dependencies": [
        {
            "name": "events",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productAR AS
	WITH productARPartialKpis AS (
		SELECT
			e.shipper,
			e.product,
			COALESCE(e.CON016, 0) AS PreseInCarico,
			COALESCE(e.RECRN001A, 0) AS Consegnate,
			COALESCE(e.RECRN001C, 0) / COALESCE(e.RECRN001A, 0) AS PercentualeDematConsegnate,
			COALESCE(e.RECRN002A, 0) AS MancatoRecapito,
			COALESCE(e.RECRN002C, 0) / COALESCE(e.RECRN002A, 0) AS PercentualeDematMancatoRecapito,
			COALESCE(e.RECRN002D, 0) AS Irreperibile,
			COALESCE(e.RECRN002F, 0) / COALESCE(e.RECRN002D, 0) AS PercentualeDematIrreperibile,
			COALESCE(e.RECRN010, 0) AS Inesito,
			COALESCE(e.RECRN011, 0) AS InGiacenza,
			COALESCE(e.RECRN003A, 0) AS ConsegnateInGiacenza,
			COALESCE(e.RECRN003C, 0) / COALESCE(e.RECRN003A, 0) AS PercentualeDematConsegnateInGiacenza,
			COALESCE(e.RECRN004A, 0) AS MancataConsegnaInGiacenza,
			COALESCE(e.RECRN004C, 0) / COALESCE(e.RECRN004A, 0) AS PercentualeDematMancataConsegnaInGiacenza,
			COALESCE(e.RECRN005A, 0) AS CompiutaGiacenza,
			COALESCE(e.RECRN005C, 0) / COALESCE(e.RECRN005A, 0) AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECRN006, 0) + COALESCE(e.RECRN013, 0) AS NonRendicontabili
		FROM events e
		WHERE e.product = "AR"
	), productARKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM productARPartialKpis p
	) SELECT * FROM productARKpis
;

/*
$QueryMetadata
{
    "name": "productRS",
    "dependencies": [
        {
            "name": "events",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRS AS
	WITH productRSPartialKpis AS (
		SELECT
			e.shipper,
			e.product,
			COALESCE(e.CON016, 0) AS PreseInCarico,
			COALESCE(e.RECRS001C, 0) AS Consegnate,
			"NA" AS PercentualeDematConsegnate,
			COALESCE(e.RECRS002A, 0) AS MancatoRecapito,
			COALESCE(e.RECRS002C, 0) / COALESCE(e.RECRS002A, 0) AS PercentualeDematMancatoRecapito,
			COALESCE(e.RECRS002D, 0) AS Irreperibile,
			COALESCE(e.RECRS002F, 0) / COALESCE(e.RECRS002D, 0) AS PercentualeDematIrreperibile,
			COALESCE(e.RECRS010, 0) AS Inesito,
			COALESCE(e.RECRS011, 0) AS InGiacenza,
			COALESCE(e.RECRS003C, 0) AS ConsegnateInGiacenza,
			"NA" AS PercentualeDematConsegnateInGiacenza,
			COALESCE(e.RECRS004A, 0) AS MancataConsegnaInGiacenza,
			COALESCE(e.RECRS004C, 0) / COALESCE(e.RECRS004A, 0) AS PercentualeDematMancataConsegnaInGiacenza,
			COALESCE(e.RECRS005A, 0) AS CompiutaGiacenza,
			COALESCE(e.RECRS005C, 0) / COALESCE(e.RECRS005A, 0) AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECRS006, 0) + COALESCE(e.RECRS013, 0) AS NonRendicontabili
		FROM events e
		WHERE e.product = "RS"
	), productRSKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM productRSPartialKpis p
	) SELECT * FROM productRSKpis
;

/*
$QueryMetadata
{
    "name": "ShipperReliabilityReport",
    "dependencies": [
        {
            "name": "product890",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        },
        {
            "name": "productAR",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        },
        {
            "name": "productRS",
            "location": "analog-delivery-monitoring/queries/ShipperReliabilityReport.sql"
        }
    ]
}
*/
SELECT * FROM product890 p890
UNION ALL
SELECT * FROM productAR pAR
UNION ALL
SELECT * FROM productRS pRS
;
	
	
	
	
	
	
	
	
	
	