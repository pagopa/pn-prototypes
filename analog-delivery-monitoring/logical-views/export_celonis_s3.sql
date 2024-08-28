/*
$QueryMetadata
{
    "name": "set_utc_tmezone",
    "dependencies": [
        {
            "name": "completeUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/completeUpdatedEcMetadata.sql"
        }
    ]
}
*/
SET spark.sql.session.timeZone = Z
;

/*
$QueryMetadata
{
    "name": "selectedUpdatedEcMetadata",
    "dependencies": [
        {
            "name": "completeUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/completeUpdatedEcMetadata.sql"
        },
        {
            "name": "set_utc_tmezone",
            "location": "analog-delivery-monitoring/logical-views/export_celonis_s3.sql"
        },
    ]
}
*/
create or replace temporary view selectedUpdatedEcMetadata as
SELECT
  *,
  replace(date_format (
      to_timestamp( ec_metadata.Metadata_WriteTimestampMicros / (1000 * 1000) ),
      "yyyy-MM-dd'T'HH:mm:ssx"
  ), '+00', 'Z')
   as dynamo_update_date
from
  completeUpdatedEcMetadata
WHERE
      dynamoExportName 
    ==
      date_format(
        date_sub( now(), 1),
        "'inc_export_'yyyyMMdd"
      )
;
