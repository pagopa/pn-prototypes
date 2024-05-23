/*
$QueryMetadata
{
    "name": "SlaReport",
    "dependencies": [
        {
            "name": "completeUpdatedEcMetadata",
            "location": "analog-delivery-monitoring/logical-views/completeUpdatedEcMetadata.sql"
        }
    ]
}
*/
WITH kpi AS (
	SELECT
		c.semplified_timeline_details.paid as ente_id,
		iun,
		replace(c.paper_request_id, 'pn-cons-000~', '') AS requestId,
	    left(c.ec_metadata.requestTimestamp, 16) as requestDateTime,
	    c.ec_metadata.paperMeta_productType AS prodotto,
	    c.costi_recapito.geokey,
		c.costi_recapito.recapitista,
		c.costi_recapito.lotto,
		CAST(c.costi_recapito.costo_scaglione AS int ) AS costo_scaglione,
		CAST(c.costi_recapito.costo_demat AS int ) AS costo_demat,
		CAST(c.costi_recapito.costo_plico AS int ) AS costo_plico,
		CAST(c.costi_recapito.costo_foglio AS int ) AS costo_foglio,
		cast(c.semplified_timeline_details.paper_analogCost as int ) as prezzo_ente,
		cast(c.semplified_timeline_details.paper_envelopeWeight as int ) as grammatura,
		cast(c.semplified_timeline_details.paper_numberOfPages as int ) as numero_pagine,
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
	        e -> e.paperProg_statusCode in ('CON993','CON995','CON996','CON997','CON998')
	        ),
	      e ->  named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime',left(e.paperProg_statusDateTime, 16)
	      )
	    ),1) as scarto_consolidatore,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='CON080'),
	       	e ->  left(e.paperProg_statusDateTime, 16)
	    ),-1) as stampa_imbustamento_statusDateTime,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='CON016'),
	      	e ->  left(e.paperProg_statusDateTime, 16)
	    ),-1) as affido_recapitista_statusDateTime,
	    element_at(transform(
	      filter(c.ec_metadata.event_list, e -> e.paperProg_statusCode=='CON018'),
	        e ->  left(e.paperProg_statusDateTime, 16)
	    ),-1) as accettazione_recapitista_statusDateTime,
	    element_at(transform(
	      filter(c.ec_metadata.event_list,
	      	e -> e.paperProg_statusCode in ( 'RECRS001C', 'RECRS002A','RECRS002D','RECRN001A','RECRN002A','RECRN002D','RECAG001A','RECAG002A','RECAG003A','RECAG003D',
	       									 'RECRS010', 'RECNS010', 'RECAG010',
	       									 'RECRS006', 'RECRS013', 'RECRN006', 'RECRN013', 'RECAG004', 'RECAG013',
													 'RECRSI001', 'RECRI001', 'RECRSI005', 'RECRI005'
	       								)
	      ),
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as tentativo_recapito,
	    element_at(transform(
	      filter(c.ec_metadata.event_list,
	      	e -> e.paperProg_statusCode in ( 'RECRS011', 'RECRN011', 'RECAG011A',
													 								'RECRSI002', 'RECRI002')
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
	      									'RECRS003C','RECRS004A','RECRS005A','RECRN003A','RECRN004A','RECRN005A', 'RECAG005A','RECAG006A','RECAG007A','RECAG008A',
	      									'RECRS006', 'RECRS013', 'RECRN006', 'RECRN013', 'RECAG004', 'RECAG013',
													'RECRSI003C', 'RECRSI004A', 'RECRSI005', 'RECRI003A', 'RECRI004A', 'RECRI005'
	      									)
	      ),
	      e -> named_struct(
	        'statusCode', e.paperProg_statusCode,
					'deliveryFailureCause', e.paperProg_deliveryFailureCause,
	        'statusDateTime', left(e.paperProg_statusDateTime, 16),
	        'rendicontazioneDateTime', left(e.paperProg_clientRequestTimeStamp, 16)
	      )
	    ),-1) as certificazione_recapito,
	    element_at(transform(
	      filter(c.ec_metadata.event_list,
	      	e -> e.paperProg_statusCode in ('RECRS001C','RECRS002C','RECRS002F','RECRN001C','RECRN002C','RECRN002F', 'RECAG001C','RECAG002C','RECAG003C','RECAG003F',
	      									'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C', 'RECAG005C','RECAG006C','RECAG007C','RECAG008C',
	      									'RECRS006', 'RECRS013', 'RECRN006', 'RECRN013', 'RECAG004', 'RECAG013',
													'RECRSI003C', 'RECRSI004C', 'RECRSI005', 'RECRI003C', 'RECRI004C', 'RECRI005'
	      								    )
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
	FROM complete_updated_ec_metadata c
	WHERE c.ec_metadata.paperMeta_productType IN ('890', 'AR', 'RS', 'RIS', 'RIR' )
) SELECT
	ente_id,
	iun,
	requestID,
	requestDateTime,
	prodotto,
	geokey,
	recapitista,
	lotto,
	codiceOggetto,
	prezzo_ente,
	grammatura,
	numero_pagine,
	costo_plico + (numero_pagine - 1 ) *  costo_foglio AS costo_consolidatore,
	costo_scaglione + costo_demat as costo_recapitista,
	from_utc_timestamp(affido_consolidatore.statusDateTime, "CET") AS affido_consolidatore_data,
	from_utc_timestamp(stampa_imbustamento_statusDateTime, "CET") AS stampa_imbustamento_CON080_data,
	from_utc_timestamp(affido_recapitista_statusDateTime, "CET") AS affido_recapitista_CON016_data,
	from_utc_timestamp(accettazione_recapitista_statusDateTime, "CET") AS accettazione_recapitista_CON018_data,
	scarto_consolidatore.statusCode AS scarto_consolidatore_stato,
	from_utc_timestamp(scarto_consolidatore.statusDateTime, "CET") AS scarto_consolidatore_data,
	tentativo_recapito.statusCode AS tentativo_recapito_stato,
	from_utc_timestamp(tentativo_recapito.statusDateTime, "CET") AS tentativo_recapito_data,
	from_utc_timestamp(tentativo_recapito.rendicontazioneDateTime, "CET") AS tentativo_recapito_data_rendicontazione,
	messaingiacenza.statusCode AS messaingiacenza_recapito_stato,
	from_utc_timestamp(messaingiacenza.statusDateTime, "CET") AS messaingiacenza_recapito_data,
	from_utc_timestamp(messaingiacenza.rendicontazioneDateTime, "CET") AS messaingiacenza_recapito_data_rendicontazione,
	certificazione_recapito.statusCode AS certificazione_recapito_stato,
	certificazione_recapito.deliveryFailureCause AS certificazione_recapito_dettagli,
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
;