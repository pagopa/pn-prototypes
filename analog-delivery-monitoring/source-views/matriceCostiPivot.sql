/*
$QueryMetadata
{
    "name": "matrice_costi_2023",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_2023
USING csv
OPTIONS (
  path "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_2023_pivot.csv.gz",
  header true
);


/*
$QueryMetadata
{
    "name": "matrice_costi_202408",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202408
USING csv
OPTIONS (
  path "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_202408_pivot_v202411.csv.gz",
  header true
);

/*
$QueryMetadata
{
    "name": "matrice_costi_202412",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202412
USING csv
OPTIONS (
  path   "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_20241216_pivot.csv.gz",
  header true
);

/*
$QueryMetadata
{
    "name": "matrice_costi_202502",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202502
USING csv
OPTIONS (
  path   "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_20250201_pivot.csv.gz",
  delimiter ";",
  header true
);

/*
$QueryMetadata
{
    "name": "matrice_costi_202504",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202504
USING csv
OPTIONS (
  path   "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_20250401_pivot.csv.gz",
  delimiter ";",
  header true
);


/*
$QueryMetadata
{
    "name": "matrice_costi_202506",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202506
USING csv
OPTIONS (
  path   "s3a://${CORE_BUCKET}/external/matrice_costi/matrice_costi_20250616_v1.0.csv.gz",
  delimiter ";",
  header true
);


/*
$QueryMetadata
{
    "name": "matrice_costi_202507",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_202507
USING csv
OPTIONS (
  path   "s3a://${CORE_BUCKET}/external/matrice_costi/Matrice Costi_20250728_v1.0.csv.gz",
  delimiter ";",
  header true
);


/*
$QueryMetadata
{
    "name": "matrice_costi",
    "persist": false,
    "dependencies": [
    	{
            "name": "matrice_costi_2023",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        },
        {
            "name": "matrice_costi_202408",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        },
        {
            "name": "matrice_costi_202412",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        },
        {
            "name": "matrice_costi_202502",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        },
        {
            "name": "matrice_costi_202504",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"

        },
        {
            "name": "matrice_costi_202506",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        },
        {
            "name": "matrice_costi_202507",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        }
    ]
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi AS (
	SELECT  *, 
            '1970-01-01T00:00:00.000Z' AS startDate, 
            '2024-07-31T21:59:59.999Z' AS endDate, 
            'matrice_costi_2023' AS tenderVersion 
        FROM matrice_costi_2023
 	UNION ALL
 	SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            '2024-12-16T07:59:59.999Z' AS endDate, 
            'matrice_costi_202408' AS tenderVersion 
        FROM matrice_costi_202408
    UNION ALL
    SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            '2025-01-31T22:59:59.999Z' AS endDate,  
            'matrice_costi_202412' AS tenderVersion 
        FROM matrice_costi_202412
    UNION ALL
    SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            '2025-03-31T21:59:59.999Z' AS endDate,
            'matrice_costi_202502' AS tenderVersion 
        FROM matrice_costi_202502
    UNION ALL
    SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            '2025-06-15T21:59:59.999Z' AS endDate,
            'matrice_costi_202504' AS tenderVersion 
        FROM matrice_costi_202504
    UNION ALL
    SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            '2025-07-27T21:59:59.999Z' AS endDate,
            'matrice_costi_202506' AS tenderVersion 
        FROM matrice_costi_202506
    UNION ALL
    SELECT  geokey, 
            product, 
            recapitista, 
            lotto, 
            costo_plico, 
            costo_foglio, 
            costo_demat, 
            min, 
            max, 
            costo, 
            costo_base_20gr, 
            startDate,
            endDate,
            'matrice_costi_202507' AS tenderVersion 
        FROM matrice_costi_202507
);