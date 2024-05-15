/*
$QueryMetadata
{
    "name": "matrice_costi",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW matrice_costi
USING csv
OPTIONS (
  path "s3a://${CORE_BUCKET}/external/matrice_costi_2023_pivot.csv.gz",
  header true
);