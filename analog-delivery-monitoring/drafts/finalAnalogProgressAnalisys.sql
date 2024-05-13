/**
DEPENDENCIES:
  - /logical-views/analogProcess.sql
  - /logical-views/completeUpdatedEcMetadata.sql
*/


create or replace temporary view final_analog_progress_analisys as
WITH max_sendRequest AS (
	SELECT
		p.sendrequestid,
		max(c.paper_request_id) AS max_paper_request_id
	FROM analog_process p LEFT JOIN complete_updated_ec_metadata c
	ON c.timelineElementId = p.sendrequestid
	GROUP BY sendrequestid
), last_complete_updated_ec_metadata AS (
	SELECT c.*
	FROM complete_updated_ec_metadata c JOIN max_sendRequest m
	ON c.paper_request_id = m.max_paper_request_id
)
SELECT
    p.paid,
    p.iun,
    p.notificationsentat,
    p.`year`,
    p.`month`,
    p.`date`,
    p.prepareRequestId,
    p.sendrequestid,
    c.paper_request_id,
    array_join(transform(
                       filter(
                               c.ec_metadata.event_list,
                               e -> e.paperProg_statusCode rlike 'CON080|CON016|(CON9.*)|(RECRN.*)|(RECAG.*)|(RECRS.*)|(P.*)|(RECRSI.*)|(RECRI.*)'
                       ),
                       e -> e.paperProg_statusCode
               ),
               ' ')
                                        as business_statuses_string,
    array_join(
            array_distinct(
                    flatten(
                            transform(
                                    filter(
                                            c.ec_metadata.event_list,
                                            e -> e.paperProg_statusCode rlike '(REC.*B)|(REC.*E)'
                                    ).paperProg_attachments,
                                    e -> e.documentType
                            )
                    )
            ),
            ' ') as attachments,
    array_join(
            array_distinct(
                    transform(
                            filter(
                                    c.ec_metadata.event_list,
                                    e -> e.paperProg_statusCode not in ('RECAG012', 'REC090')
                            ),
                            e -> e.paperProg_registeredLetterCode
                    )
            ), ' ') as registeredLetterCode,
    c.ec_metadata.paperMeta_productType as prodotto,
    c.costi_recapito.recapitista,
    c.costi_recapito.lotto
FROM analog_process p LEFT JOIN last_complete_updated_ec_metadata c ON c.timelineElementId = p.sendrequestid;