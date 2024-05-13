/*
$QueryMetadata
{
    "name": "pivotedEvents",
    "dependencies": [
        {
            "name": "rankedEvents",
            "location": "analog-delivery-monitoring/logical-views/rankedEvents.sql"
        }
    ]
}
*/
create or replace temporary view pivotedEvents as
	WITH lastEvents AS (
		SELECT e.requestId, e.shipper, e.product, e.lot, e.status, max(e.ranking)
		FROM rankedEvents e
		WHERE e.status RLIKE("(CON016)|(REC.*)")
		GROUP BY e.requestId, e.shipper, e.product, e.lot, e.status
	), eventsGroupedByShipperProduct AS (
		SELECT e.shipper, e.product, e.status, COUNT(*) AS _count
		FROM lastEvents e
		GROUP BY e.shipper, e.product, e.status
	), pivoted AS (
		SELECT * FROM eventsGroupedByShipperProduct
		PIVOT
		(
			first(_count)
			FOR status IN (
				'CON016',
				'RECRN001A', 'RECRN002A', 'RECRN003A', 'RECRN004A', 'RECRN005A', 'RECRN001C', 'RECRN002C', 'RECRN003C', 'RECRN004C', 'RECRN005C', 'RECRN002D', 'RECRN002F', 'RECRN006', 'RECRN010', 'RECRN011', 'RECRN013',
				'RECAG001A', 'RECAG002A', 'RECAG003A', 'RECAG005A', 'RECAG006A', 'RECAG007A', 'RECAG008A', 'RECAG001C', 'RECAG002C', 'RECAG003C', 'RECAG005C', 'RECAG006C', 'RECAG007C', 'RECAG008C', 'RECAG003D', 'RECAG003F', 'RECAG004', 'RECAG010', 'RECAG011A', 'RECAG013',
				'RECRS002A', 'RECRS004A', 'RECRS005A', 'RECRS001C', 'RECRS002C', 'RECRS003C', 'RECRS004C', 'RECRS005C', 'RECRS002D', 'RECRS002F', 'RECRS006', 'RECRS010', 'RECRS011', 'RECRS013',
				'RECRI003A', 'RECRI004A', 'RECRI003C', 'RECRI004C', 'RECRI005',
				'RECRSI003C', 'RECRSI004A', 'RECRSI004C', 'RECRSI005'
			)
		)
	) SELECT * FROM pivoted
;

/*
$QueryMetadata
{
    "name": "product890",
    "dependencies": [
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW product890 AS
	WITH product890CalculateKpis AS (
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
		FROM pivotedEvents e
		WHERE e.product = "890"
	), product890PartialKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM product890CalculateKpis p
	), product890Kpis AS (
		SELECT
			p.*,
			(p.Consegnate + p.ConsegnateInGiacenza) / p.PreseInCarico AS PercentualeConsegna,
			(p.MancatoRecapito + p.MancataConsegnaInGiacenza) / p.PreseInCarico AS PercentualeMancataConsegna,
			p.CompiutaGiacenza / p.PreseInCarico AS PercentualeCompiutaGiacenza,
			p.Irreperibile / p.PreseInCarico AS PercentualeIrreperibilita,
			p.NonRendicontabili / p.PreseInCarico AS PercentualeNonRendicontabili,
			(p.NonInviati + p.GiacenzeInCorso) / p.PreseInCarico AS PercentualeInCorso
		FROM product890PartialKpis p
	) SELECT * FROM product890Kpis
;

/*
$QueryMetadata
{
    "name": "productAR",
    "dependencies": [
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productAR AS
	WITH productARCalculateKpis AS (
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
		FROM pivotedEvents e
		WHERE e.product = "AR"
	), productARPartialKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM productARCalculateKpis p
	), productARKpis AS (
		SELECT
			p.*,
			(p.Consegnate + p.ConsegnateInGiacenza) / p.PreseInCarico AS PercentualeConsegna,
			(p.MancatoRecapito + p.MancataConsegnaInGiacenza) / p.PreseInCarico AS PercentualeMancataConsegna,
			p.CompiutaGiacenza / p.PreseInCarico AS PercentualeCompiutaGiacenza,
			p.Irreperibile / p.PreseInCarico AS PercentualeIrreperibilita,
			p.NonRendicontabili / p.PreseInCarico AS PercentualeNonRendicontabili,
			(p.NonInviati + p.GiacenzeInCorso) / p.PreseInCarico AS PercentualeInCorso
		FROM productARPartialKpis p
	) SELECT * FROM productARKpis
;

/*
$QueryMetadata
{
    "name": "productRS",
    "dependencies": [
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRS AS
	WITH productRSCalculateKpis AS (
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
		FROM pivotedEvents e
		WHERE e.product = "RS"
	), productRSPartialKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.Irreperibile + p.Inesito + p.NonRendicontabili) AS NonInviati,
			p.Inesito - (p.ConsegnateInGiacenza + p.MancataConsegnaInGiacenza + p.CompiutaGiacenza) AS GiacenzeInCorso
		FROM productRSCalculateKpis p
	), productRSKpis AS (
		SELECT
			p.*,
			(p.Consegnate + p.ConsegnateInGiacenza) / p.PreseInCarico AS PercentualeConsegna,
			(p.MancatoRecapito + p.MancataConsegnaInGiacenza) / p.PreseInCarico AS PercentualeMancataConsegna,
			p.CompiutaGiacenza / p.PreseInCarico AS PercentualeCompiutaGiacenza,
			p.Irreperibile / p.PreseInCarico AS PercentualeIrreperibilita,
			p.NonRendicontabili / p.PreseInCarico AS PercentualeNonRendicontabili,
			(p.NonInviati + p.GiacenzeInCorso) / p.PreseInCarico AS PercentualeInCorso
		FROM productRSPartialKpis p
	) SELECT * FROM productRSKpis
;

/*
$QueryMetadata
{
    "name": "productRIR",
    "dependencies": [
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRIR AS
	WITH productRIRCalculateKpis AS (
		SELECT
			e.shipper,
			e.product,
			COALESCE(e.CON016, 0) AS PreseInCarico,
			COALESCE(e.RECRI003A, 0) AS Consegnate,
			COALESCE(e.RECRI003C, 0) / COALESCE(e.RECRI003A, 0) AS PercentualeDematConsegnate,
			COALESCE(e.RECRI004A, 0) AS MancatoRecapito,
			COALESCE(e.RECRI004C, 0) / COALESCE(e.RECRI004A, 0) AS PercentualeDematMancatoRecapito,
			"NA" AS Irreperibile,
			"NA" AS PercentualeDematIrreperibile,
			"NA" AS Inesito,
			"NA" AS InGiacenza,
			"NA" AS ConsegnateInGiacenza,
			"NA" AS PercentualeDematConsegnateInGiacenza,
			"NA" AS MancataConsegnaInGiacenza,
			"NA" AS PercentualeDematMancataConsegnaInGiacenza,
			"NA" AS CompiutaGiacenza,
			"NA" AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECRI005, 0) AS NonRendicontabili
		FROM pivotedEvents e
		WHERE e.product = "RIR"
	), productRIRPartialKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.NonRendicontabili) AS NonInviati,
			"NA" AS GiacenzeInCorso
		FROM productRIRCalculateKpis p
	), productRIRKpis AS (
		SELECT
			p.*,
			p.Consegnate / p.PreseInCarico AS PercentualeConsegna,
			p.MancatoRecapito / p.PreseInCarico AS PercentualeMancataConsegna,
			"NA" AS PercentualeCompiutaGiacenza,
			"NA" AS PercentualeIrreperibilita,
			p.NonRendicontabili / p.PreseInCarico AS PercentualeNonRendicontabili,
			p.NonInviati / p.PreseInCarico AS PercentualeInCorso
		FROM productRIRPartialKpis p
	) SELECT * FROM productRIRKpis
;

/*
$QueryMetadata
{
    "name": "productRIS",
    "dependencies": [
        {
            "name": "pivotedEvents",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRIS AS
	WITH productRISCalculateKpis AS (
		SELECT
			e.shipper,
			e.product,
			COALESCE(e.CON016, 0) AS PreseInCarico,
			COALESCE(e.RECRSI003C, 0) AS Consegnate,
			"NA" AS PercentualeDematConsegnate,
			COALESCE(e.RECRSI004A, 0) AS MancatoRecapito,
			COALESCE(e.RECRSI004C, 0) / COALESCE(e.RECRSI004A, 0) AS PercentualeDematMancatoRecapito,
			"NA" AS Irreperibile,
			"NA" AS PercentualeDematIrreperibile,
			"NA" AS Inesito,
			"NA" AS InGiacenza,
			"NA" AS ConsegnateInGiacenza,
			"NA" AS PercentualeDematConsegnateInGiacenza,
			"NA" AS MancataConsegnaInGiacenza,
			"NA" AS PercentualeDematMancataConsegnaInGiacenza,
			"NA" AS CompiutaGiacenza,
			"NA" AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECRSI005, 0) AS NonRendicontabili
		FROM pivotedEvents e
		WHERE e.product = "RIS"
	), productRISPartialKpis AS (
		SELECT
			p.*,
			p.PreseInCarico - (p.Consegnate + p.MancatoRecapito + p.NonRendicontabili) AS NonInviati,
			"NA" AS GiacenzeInCorso
		FROM productRISCalculateKpis p
	), productRISKpis AS (
		SELECT
			p.*,
			p.Consegnate / p.PreseInCarico AS PercentualeConsegna,
			p.MancatoRecapito / p.PreseInCarico AS PercentualeMancataConsegna,
			"NA" AS PercentualeCompiutaGiacenza,
			"NA" AS PercentualeIrreperibilita,
			p.NonRendicontabili / p.PreseInCarico AS PercentualeNonRendicontabili,
			p.NonInviati / p.PreseInCarico AS PercentualeInCorso
		FROM productRISPartialKpis p
	) SELECT * FROM productRISKpis
;

/*
$QueryMetadata
{
    "name": "ShipperProductReliabilityReport",
    "dependencies": [
        {
            "name": "product890",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        },
        {
            "name": "productAR",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        },
        {
            "name": "productRS",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        },
        {
            "name": "productRIR",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        },
        {
            "name": "productRIS",
            "location": "analog-delivery-monitoring/queries/ShipperProductReliabilityReport.sql"
        }
    ]
}
*/
SELECT * FROM product890 p890
UNION ALL
SELECT * FROM productAR pAR
UNION ALL
SELECT * FROM productRS pRS
UNION ALL
SELECT * FROM productRIR pRIR
UNION ALL
SELECT * FROM productRIS pRIS
;