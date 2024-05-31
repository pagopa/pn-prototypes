/*
$QueryMetadata
{
    "name": "completeUpdatedEcMetadata",
    "dependencies": [
        {
            "name": "incremental_ec_metadta__fromfile",
            "location": "analog-delivery-monitoring/source-views/pnEcRichiesteMetadati.sql"
        },
        {
            "name": "incremental_timeline",
            "location": "analog-delivery-monitoring/source-views/pnTimelines.sql"
        },
        {
            "name": "matrice_costi",
            "location": "analog-delivery-monitoring/source-views/matriceCosti2023Pivot.sql"
        }
    ]
}
*/
create or replace temporary view completeUpdatedEcMetadata as
  WITH
    last_modification_by_request_id AS (
      select
        e.requestId,
        max( coalesce( cast(e.Metadata_WriteTimestampMicros as long), 0) ) as ts
      from
        incremental_ec_metadta__fromfile e
	  WHERE
	  	paperMeta_productType is not null
      group by
        e.requestId
    ),
    ec_metadata_last_update AS (
      SELECT
          l.ts,
          e.*,
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  e.requestId,
                  '[^~]*~',
                  ''
                ),
                'PREPARE_ANALOG',
                'SEND_ANALOG'
              ),
              'PREPARE_SIMPLE_',
              'SEND_SIMPLE_'
            ),
            '(\\.)?PCRETRY_[0-9]+',
            ''
          )
           as timelineElementId_computed
        FROM
          last_modification_by_request_id l
          LEFT JOIN incremental_ec_metadta__fromfile e
                 ON e.requestId = l.requestId
                    and l.ts = coalesce( cast(e.Metadata_WriteTimestampMicros as long), 0)
    ),
    ecmetadata_with_timeline AS (
      SELECT
        e.requestid AS paper_request_id,
        t.iun AS iun,
        t.timelineElementId AS timelineElementId,
        named_struct (
          'timeline_zip', get_json_object( t.details, '$.physicalAddress.M.zip.S'),
          'timeline_state', get_json_object( t.details, '$.physicalAddress.M.foreignState.S'),
          'paper_analogCost', get_json_object( t.details, '$.analogCost.N'),
          'paper_envelopeWeight', get_json_object( t.details, '$.envelopeWeight.N'),
          'paper_numberOfPages', get_json_object( t.details, '$.numberOfPages.N'),
          'paid', t.paid
        )
         AS semplified_timeline_details,
        struct(
          e.*
        )
         as ec_metadata
      from
        ec_metadata_last_update e
        left join incremental_timeline t on e.timelineElementId_computed = t.timelineElementId
      where
        e.paperMeta_productType is not null
    ),
    ecmetadata_with_timeline_and_costi AS (
      select
        et.*,
        named_struct (
          'recapitista', c.recapitista,
          'lotto', c.lotto,
          'geokey', c.geokey,
          'costo_scaglione', c.costo,
          'costo_plico', c.costo_plico,
          'costo_foglio', c.costo_foglio,
          'costo_demat', c.costo_demat
        )
         as costi_recapito
      from
        ecmetadata_with_timeline et
        left join matrice_costi c on
            et.ec_metadata.paperMeta_productType = c.product
          and
            c.geokey =
            (
              case
                when et.ec_metadata.paperMeta_productType in ('AR', 'RS', '890') then et.semplified_timeline_details.timeline_zip
                when et.ec_metadata.paperMeta_productType in ('RIS', 'RIR') then et.semplified_timeline_details.timeline_state
                else null
              end
            )
          and
            c.min =
            (
            CASE
            	WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 1 AND 20 THEN 1
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 21 AND 50 THEN 21
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 51 AND 100 THEN 51
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 101 AND 250 THEN 101
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 251 AND 350 THEN 251
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 351 AND 1000 THEN 351
	            WHEN CAST(et.semplified_timeline_details.paper_envelopeWeight AS int) BETWEEN 1001 AND 2000 THEN 1001
	            ELSE NULL
	        END
            )
    )
select
    *
FROM
    ecmetadata_with_timeline_and_costi
where
    timelineElementId is not null
;