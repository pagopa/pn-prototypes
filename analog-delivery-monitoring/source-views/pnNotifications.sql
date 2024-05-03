/*
$QueryMetadata
{
    "name": "pnNotifications",
    "dependencies": []
}
*/
CREATE OR REPLACE temporary view incremental_notification
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://${CORE_BUCKET}/parquet/pn-Notifications/",
  "parquet.enableVectorizedReader" "false"
);