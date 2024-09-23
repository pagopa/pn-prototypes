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
    "name": "matrice_costi_2024",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi_2024
USING csv
OPTIONS (
  path "s3://${CORE_BUCKET}/external/matrice_costi/matrice_costi_202408_pivot_v202410.csv.gz",
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
            "name": "matrice_costi_2024",
            "location": "analog-delivery-monitoring/source-views/matriceCostiPivot.sql"
        }
    ]
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi AS (
	SELECT *, '1970-01-01T00:00:00.000Z' AS startDate, '2024-07-31T21:59:59.999Z' AS endDate, 'matrice_costi_2023' AS tenderVersion FROM matrice_costi_2023
 	UNION ALL
 	SELECT *, 'matrice_costi_2024' AS tenderVersion FROM matrice_costi_2024
);