/*
$QueryMetadata
{
    "name": "con996_2023",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE TEMPORARY VIEW con996_2023
USING csv
OPTIONS (
  path "s3a://${CORE_BUCKET}/external/all_con996_2023.csv",
  header true
);