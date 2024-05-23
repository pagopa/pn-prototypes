/*
$QueryMetadata
{
    "name": "incremental_ec_metadta__fromfile",
    "persist": false,
    "dependencies": []
}
*/
CREATE OR REPLACE temporary view incremental_ec_metadta__fromfile
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://${CONFINFO_BUCKET}/parquet/pn-EcRichiesteMetadati/",
  "parquet.enableVectorizedReader" "false"
);