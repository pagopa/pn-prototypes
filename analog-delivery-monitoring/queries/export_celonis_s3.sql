/*
$QueryMetadata
{
    "name": "celonis_export_paper_requests",
    "dependencies": [
        {
            "name": "selectedUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/export_celonis_s3.sql"
        }
    ]
}
*/
select 
  paper_request_id,
  semplified_timeline_details.paid as semplified_timeline__paid,
  iun,
  semplified_timeline_details.paper_numberOfPages as semplified_timeline___paper_numberOfPages,
  semplified_timeline_details.paper_envelopeWeight as semplified_timeline__paper_envelopeWeight,
  semplified_timeline_details.paper_analogCost as semplified_timeline__paper_analogCost, 
  semplified_timeline_details.timeline_zip as semplified_timeline__zip,
  semplified_timeline_details.timeline_state as semplified_timeline__state,
  semplified_timeline_details.timeline_recipient_index as semplified_timeline__recipient_index,
  semplified_timeline_details.timeline_peso_gr_min as semplified_timeline__peso_gr_min,
  ec_metadata.ts,
  if( 
    char_length(ec_metadata.lastUpdateTimestamp) == 17, 
    replace( ec_metadata.lastUpdateTimestamp, 'Z', ':00Z'), 
    ec_metadata.lastUpdateTimestamp
  ) 
    as lastUpdateTimestamp,
  ec_metadata.Metadata_WriteTimestampMicros,
  ec_metadata.requestId,
  ec_metadata.clientId,
  ec_metadata.paperMeta_productType,
  ec_metadata.paperMeta_printType,
  if( 
    char_length(ec_metadata.requestTimestamp) == 17, 
    replace( ec_metadata.requestTimestamp, 'Z', ':00Z'), 
    ec_metadata.requestTimestamp
  ) 
    as requestTimestamp,
  ec_metadata.clientRequestTimeStamp,
  ec_metadata.statusRequest,
  ec_metadata.version,
  ec_metadata.event_list_length,
  costi_recapito.costo_scaglione as costi_recapito__costo_scaglione,
  costi_recapito.costo_demat as costi_recapito__costo_demat,
  costi_recapito.costo_foglio as costi_recapito__costo_foglio,
  costi_recapito.costo_plico as costi_recapito__costo_plico,
  costi_recapito.geokey as costi_recapito__geokey,
  costi_recapito.lotto as costi_recapito__lotto,
  costi_recapito.product as costi_recapito__product,
  costi_recapito.recapitista as costi_recapito__recapitista,
  costi_recapito.costo_base_20gr as costi_recapito__costo_base_20gr, 
  costi_recapito.max as costi_recapito__gramatura_max,
  notification_year,
  notification_month,
  notification_day,
  dynamo_update_date,
  dynamoExportName
from
  selectedUpdatedEcMetadata
;



/*
$QueryMetadata
{
    "name": "celonis_export_event_list",
    "dependencies": [
        {
            "name": "selectedUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/export_celonis_s3.sql"
        }
    ]
}
*/
with events as (
  select 
    paper_request_id,
    dynamo_update_date,
    dynamoExportName,
    posexplode( ec_metadata.event_list )
  from
    selectedUpdatedEcMetadata
  )
select 
  paper_request_id || '__' || lpad( pos, 4, '0') as id,
  paper_request_id,
  pos as event_idx,
  col.paperProg_statusDescription,
  col.paperProg_deliveryFailureCause,
  if(
    char_length( col.insertTimestamp ) == 17, 
    replace( col.insertTimestamp, 'Z', ':00Z'), 
    col.insertTimestamp
  )
    as insertTimestamp,
  if( 
    char_length( col.paperProg_clientRequestTimeStamp ) == 17, 
    replace( col.paperProg_clientRequestTimeStamp, 'Z', ':00Z'), 
    col.paperProg_clientRequestTimeStamp
  )
    as paperProg_clientRequestTimeStamp,
  if( 
    char_length( col.paperProg_statusDateTime ) == 17, 
    replace( col.paperProg_statusDateTime, 'Z', ':00Z'), 
    col.paperProg_statusDateTime
  )
    as paperProg_statusDateTime,
  col.paperProg_registeredLetterCode,
  col.paperProg_productType,
  col.paperProg_status,
  col.paperProg_statusCode,
  dynamoExportName
from 
  events
;


/*
$QueryMetadata
{
    "name": "celonis_export_event_list_document_type",
    "dependencies": [
        {
            "name": "selectedUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/export_celonis_s3.sql"
        }
    ]
}
*/
with 
  events as (
    select 
      paper_request_id,
      dynamo_update_date,
      dynamoExportName,
      posexplode( ec_metadata.event_list )
    from
      selectedUpdatedEcMetadata
  ),
  events_doc_types as (
    select 
      paper_request_id,
      dynamo_update_date,
      dynamoExportName,
      pos as event_idx,
      explode( col.paperProg_attachments )
    from
      events
  )
select 
  paper_request_id || '__' || lpad( event_idx, 4, '0') || '__' || lpad( col.id, 4, '0') as id,
  paper_request_id || '__' || lpad( event_idx, 4, '0') as event_id,
  paper_request_id,
  event_idx,
  col.id as document_idx,
  case 
    when col.documentType in ('Plico AG', 'Plico CAN', 'Plico Raccomandata') then 'Plico'
    when col.documentType in ('23I') then 'AR'
    else col.documentType
  end
    as documentType,
  dynamoExportName
from 
  events_doc_types
;
