/*
$QueryMetadata
{
    "name": "pnTimelines",
    "dependencies": []
}
*/
CREATE OR REPLACE temporary view incremental_timeline
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://${CORE_BUCKET}/parquet/pn-Timelines/",
  "parquet.enableVectorizedReader" "false"
);