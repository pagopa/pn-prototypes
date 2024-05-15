CREATE temporary view incremental_timeline
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://${CORE_BUCKET}/parquet/pn-Timelines/",
  "parquet.enableVectorizedReader" "false"
)

CREATE temporary view incremental_ec_metadta__fromfile
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://${CONFINFO_BUCKET}/parquet/pn-EcRichiesteMetadati/",
  "parquet.enableVectorizedReader" "false"
)

CREATE TEMPORARY VIEW matrice_costi
USING csv 
OPTIONS (
  path "s3a://${CORE_BUCKET}/external/matrice_costi_2023_pivot.csv.gz",
  header true
)


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
          'geokey', c.geokey
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
            c.min = 1            
    )
  select
    *
  FROM
    ecmetadata_with_timeline_and_costi
  where 
    timelineElementId is not null
; 

WITH kpi AS (
	SELECT 
		iun,
		replace(c.paper_request_id, 'pn-cons-000~', '') AS requestId,
	    left(c.ec_metadata.requestTimestamp, 16) as requestDateTime,
	    c.ec_metadata.paperMeta_productType AS prodotto,
	    c.costi_recapito.geokey,
		c.costi_recapito.recapitista,
		c.costi_recapito.lotto,
		rtrim(array_join(
		     array_distinct(
		        transform(
		          filter(
		            c.ec_metadata.event_list,
						e -> e.paperProg_statusCode not in ('REC090', 'RECAG012')
		            ),
		              e -> e.paperProg_registeredLetterCode
		            )
		), ' ')) as codiceOggetto,
	    array_join(transform(
	      filter(
	        c.ec_metadata.event_list, 
	        e -> e.paperProg_statusCode rlike 'CON080|CON016|(CON9.*)|(RECRN.*)|(RECAG.*)|(RECRS.*)|(P.*)|(RECRSI.*)|(RECRI.*)'
	      ),
	      e -> e.paperProg_statusCode
	    ), ' ')
	     as statuses_string,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='P000'), 
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16)
	      )
	    ),1) as affido_consolidatore,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, 
	        e -> e.paperProg_statusCode in ('CON993','CON995','CON996','CON997','CON998','P010')
	        ), 
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime',left(e.paperProg_statusDateTime, 16)
	      )
	    ),1) as scarto_consolidatore,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='CON016'), 
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16)
	      )
	    ),-1) as affido_recapitista,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, 
	      	e -> e.paperProg_statusCode in ( 'RECRS001C', 'RECRS002A','RECRS002D','RECRN001A','RECRN002A','RECRN002D','RECAG001A','RECAG002A','RECAG003A','RECAG003D', 
	       									'RECRS006', 'RECRS010', 'RECNS010', 'RECRN006', 'RECAG004', 'RECAG010' )
	      ), 
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as tentativo_recapito,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, 
	      	e -> e.paperProg_statusCode in ( 'RECRS011', 'RECRN011', 'RECAG011A')
	      ), 
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as  messaingiacenza,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, 
	      	e -> e.paperProg_statusCode in ('RECRS001C','RECRS002A','RECRS002D','RECRN001A','RECRN002A','RECRN002D', 'RECAG001A','RECAG002A','RECAG003A','RECAG003D',
	      									'RECRS003C','RECRS004A','RECRS005A','RECRN003A','RECRN004A','RECRN005A', 'RECAG005A','RECAG006A','RECAG007A','RECAG008A' )
	      ), 
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as certificazione_recapito,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, 
	      	e -> e.paperProg_statusCode in ('RECRS001C','RECRS002C','RECRS002F','RECRN001C','RECRN002C','RECRN002F', 'RECAG001C','RECAG002C','RECAG003C','RECAG003F',
	      									'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C', 'RECAG005C','RECAG006C','RECAG007C','RECAG008C' )
	      ), 
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as fine_recapito,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='RECAG012'), 
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as accettazione_23L,
	    element_at(transform(
	      filter(
	      	c.ec_metadata.event_list, 
	      	e -> (e.paperProg_attachments[0].documentType=='23L' or e.paperProg_attachments[1].documentType=='23L' OR e.paperProg_attachments[2].documentType=='23L' ) 
	      ), 
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as demat_23L
	FROM completeUpdatedEcMetadata c
	WHERE c.ec_metadata.paperMeta_productType IN ('890', 'AR', 'RS' )
	AND left(c.ec_metadata.requestTimestamp, 7) < "2023-10"
) SELECT 
	iun,
	requestID,
	requestDateTime,
	prodotto,
	geokey,
	lotto,
	codiceOggetto,
	from_utc_timestamp(affido_consolidatore.statusDateTime, "CET") AS affido_consolidatore_data,
	from_utc_timestamp(affido_recapitista.statusDateTime, "CET") AS affido_recapitista_CON016_data,
	tentativo_recapito.statusCode AS tentativo_recapito_stato,
	from_utc_timestamp(tentativo_recapito.statusDateTime, "CET") AS tentativo_recapito_data,
	from_utc_timestamp(tentativo_recapito.rendicontazioneDateTime, "CET") AS tentativo_recapito_data_rendicontazione,
	messaingiacenza.statusCode AS messaingiacenza_recapito_stato,
	from_utc_timestamp(messaingiacenza.statusDateTime, "CET") AS messaingiacenza_recapito_data,
	from_utc_timestamp(messaingiacenza.rendicontazioneDateTime, "CET") AS messaingiacenza_recapito_data_rendicontazione,
	certificazione_recapito.statusCode AS certificazione_recapito_stato,
	from_utc_timestamp(certificazione_recapito.statusDateTime, "CET") AS certificazione_recapito_data,
	from_utc_timestamp(certificazione_recapito.rendicontazioneDateTime, "CET") AS certificazione_recapito_data_rendicontazione,
	fine_recapito.statusCode AS fine_recapito_stato,
	from_utc_timestamp(fine_recapito.statusDateTime, "CET") AS fine_recapito_data,
	from_utc_timestamp(fine_recapito.rendicontazioneDateTime, "CET") AS fine_recapito_data_rendicontazione,
	from_utc_timestamp(accettazione_23L.statusDateTime, "CET") AS accettazione_23L_RECAG012_data,
	from_utc_timestamp(accettazione_23L.rendicontazioneDateTime, "CET") AS accettazione_23L_RECAG012_data_rendicontazione,
	demat_23L.statusCode AS demat_23L_stato,
	from_utc_timestamp(demat_23L.statusDateTime, "CET") AS demat_23L_data,
	from_utc_timestamp(demat_23L.rendicontazioneDateTime, "CET") AS demat_23L_data_rendicontazione
FROM 
	kpi
WHERE 
	scarto_consolidatore IS NULL
;



SELECT 
	requestId,
	from_utc_timestamp(LEFT(requestTimestamp,16), "CET"),
	 element_at(transform(
	      filter(event_list, e -> (e.paperProg_attachments[0].documentType=='23L' or e.paperProg_attachments[1].documentType=='23L' OR e.paperProg_attachments[2].documentType=='23L' ) ), 
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as pippo
FROM incremental_ec_metadta__fromfile
;