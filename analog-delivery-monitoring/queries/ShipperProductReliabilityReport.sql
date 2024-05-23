/*
$QueryMetadata
{
    "name": "product890",
    "dependencies": [
        {
            "name": "groupedPivotEvents",
            "location": "analog-delivery-monitoring/logical-views/groupedPivotEvents.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW product890 AS
	WITH product890Aggregates AS (
		SELECT 
			e.shipper, 
			e.product,
			sum(e.CON016) as CON016,
			sum(e.RECAG001A) as RECAG001A,
			sum(e.RECAG002A) as RECAG002A,
			sum(e.RECAG003A) as RECAG003A,
			sum(e.RECAG005A) as RECAG005A,
			sum(e.RECAG006A) as RECAG006A,
			sum(e.RECAG007A) as RECAG007A,
			sum(e.RECAG008A) as RECAG008A,
			sum(e.RECAG011A) as RECAG011A,
			sum(e.RECAG001C) as RECAG001C,
			sum(e.RECAG002C) as RECAG002C,
			sum(e.RECAG003C) as RECAG003C,
			sum(e.RECAG005C) as RECAG005C,
			sum(e.RECAG006C) as RECAG006C,
			sum(e.RECAG007C) as RECAG007C,
			sum(e.RECAG008C) as RECAG008C,
			sum(e.RECAG003D) as RECAG003D,
			sum(e.RECAG003F) as RECAG003F,
			sum(e.RECAG010) as RECAG010,
			sum(e.RECAG004) as RECAG004,
			sum(e.RECAG013) as RECAG013
		FROM groupedPivotEvents e
		WHERE e.product = "890"
		GROUP BY e.shipper, e.product
	), product890CalculateKpis AS (
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
			COALESCE(e.RECAG005A, 0) + COALESCE(e.RECAG006A, 0) AS ConsegnateInGiacenza,
			(COALESCE(e.RECAG005C, 0) + COALESCE(e.RECAG006C, 0)) / (COALESCE(e.RECAG005A, 0) + COALESCE(e.RECAG006A, 0)) AS PercentualeDematConsegnateInGiacenza,
			COALESCE(e.RECAG007A, 0) AS MancataConsegnaInGiacenza,
			COALESCE(e.RECAG007C, 0) / COALESCE(e.RECAG007A, 0) AS PercentualeDematMancataConsegnaInGiacenza,
			COALESCE(e.RECAG008A, 0) AS CompiutaGiacenza,
			COALESCE(e.RECAG008C, 0) / COALESCE(e.RECAG008A, 0) AS PercentualeDematCompiutaGiacenza,
			COALESCE(e.RECAG004, 0) + COALESCE(e.RECAG013, 0) AS NonRendicontabili
		FROM product890Aggregates e
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
            "name": "groupedPivotEvents",
            "location": "analog-delivery-monitoring/logical-views/groupedPivotEvents.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productAR AS
	WITH productARAggregates AS (
		SELECT 
			e.shipper, 
			e.product,
			sum(e.CON016) as CON016,
			sum(e.RECRN001A) as RECRN001A,
			sum(e.RECRN002A) as RECRN002A,
			sum(e.RECRN003A) as RECRN003A,
			sum(e.RECRN004A) as RECRN004A,
			sum(e.RECRN005A) as RECRN005A,
			sum(e.RECRN001C) as RECRN001C,
			sum(e.RECRN002C) as RECRN002C,
			sum(e.RECRN003C) as RECRN003C,
			sum(e.RECRN004C) as RECRN004C,
			sum(e.RECRN005C) as RECRN005C,
			sum(e.RECRN002D) as RECRN002D,
			sum(e.RECRN002F) as RECRN002F,
			sum(e.RECRN010) as RECRN010,
			sum(e.RECRN011) as RECRN011,
			sum(e.RECRN006) as RECRN006,
			sum(e.RECRN013) as RECRN013
		FROM groupedPivotEvents e
		WHERE e.product = "AR"
		GROUP BY e.shipper, e.product
	), productARCalculateKpis AS (
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
		FROM productARAggregates e
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
            "name": "groupedPivotEvents",
            "location": "analog-delivery-monitoring/logical-views/groupedPivotEvents.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRS AS
	WITH productRSAggregates AS (
		SELECT 
			e.shipper, 
			e.product,
			sum(e.CON016) as CON016,
			sum(e.RECRS002A) as RECRS002A,
			sum(e.RECRS004A) as RECRS004A,
			sum(e.RECRS005A) as RECRS005A,
			sum(e.RECRS001C) as RECRS001C,
			sum(e.RECRS002C) as RECRS002C,
			sum(e.RECRS003C) as RECRS003C,
			sum(e.RECRS004C) as RECRS004C,
			sum(e.RECRS005C) as RECRS005C,
			sum(e.RECRS002D) as RECRS002D,
			sum(e.RECRS002F) as RECRS002F,
			sum(e.RECRS010) as RECRS010,
			sum(e.RECRS011) as RECRS011,
			sum(e.RECRS006) as RECRS006,
			sum(e.RECRS013) as RECRS013
		FROM groupedPivotEvents e
		WHERE e.product = "RS"
		GROUP BY e.shipper, e.product
	), productRSCalculateKpis AS (
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
		FROM productRSAggregates e
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
            "name": "groupedPivotEvents",
            "location": "analog-delivery-monitoring/logical-views/groupedPivotEvents.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRIR AS
	WITH productRIRAggregates AS (
		SELECT 
			e.shipper, 
			e.product,
			sum(e.CON016) as CON016,
			sum(e.RECRI003A) as RECRI003A,
			sum(e.RECRI004A) as RECRI004A,
			sum(e.RECRI003C) as RECRI003C,
			sum(e.RECRI004C) as RECRI004C,
			sum(e.RECRI005) as RECRI005
		FROM groupedPivotEvents e
		WHERE e.product = "RIR"
		GROUP BY e.shipper, e.product
	), productRIRCalculateKpis AS (
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
		FROM productRIRAggregates e
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
            "name": "groupedPivotEvents",
            "location": "analog-delivery-monitoring/logical-views/groupedPivotEvents.sql"
        }
    ]
}
*/
CREATE OR replace TEMPORARY VIEW productRIS AS
	WITH productRISAggregates AS (
		SELECT 
			e.shipper, 
			e.product,
			sum(e.CON016) as CON016,
			sum(e.RECRSI004A) as RECRSI004A,
			sum(e.RECRSI003C) as RECRSI003C,
			sum(e.RECRSI004C) as RECRSI004C,
			sum(e.RECRSI005) as RECRSI005
		FROM groupedPivotEvents e
		WHERE e.product = "RIS"
		GROUP BY e.shipper, e.product
	), productRISCalculateKpis AS (
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
		FROM productRISAggregates e
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