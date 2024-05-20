CREATE or replace temporary view exported_data_reloaded
    USING org.apache.spark.sql.parquet
    OPTIONS (
    path "s3a://pn-datamonitoring-eu-south-1-510769970275/parquet/generated/pn9391_431_notification_with_related_ecmetadata"
);

with
    ecmetadata_enriched as (
        select
            filter (
                        event_list_codes,
                        c -> c in ('CON993','CON995','CON996','CON997','CON998')
            ) as exp_scarti_consolidatore,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS001C','RECRN001A','RECAG001A','RECAG002A','RECRSI003C','RECRI003A')
            ) as exp_atto_consegnato_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS002A','RECRN002A','RECAG003A', 'RECRSI004A', 'RECRI004A')
            ) as exp_atto_non_consegnato_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS002D','RECRN002D','RECAG003D')
            ) as exp_atto_non_consegnato_irrep_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS003C','RECRS004A','RECRS005A','RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECAG006A','RECAG007A','RECAG008A')
            ) as exp_giacenza_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS010', 'RECRN010', 'RECAG010','RECRS011', 'RECRN011', 'RECAG011A')
            ) as exp_inesito,
            filter (
                        event_list_codes,
                        c -> c in ( 'RECRSI001', 'RECRI001', 'RECRSI002', 'RECRI002')
            ) as exp_avvio_internazionale,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013','RECRSI005','RECRI005')
            ) as exp_errori_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS001C','RECRN001C','RECAG001C','RECAG002C','RECRSI003C','RECRI003C')
            ) as exp_atto_consegnato_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS002C','RECRN002C','RECAG003C','RECRSI004C','RECRI004C')
            ) as exp_atto_non_consegnato_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS002F','RECRN002F','RECAG003F')
            ) as exp_atto_non_consegnato_irrep_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            ) as exp_giacenza_esito,
            filter (
                        event_list_codes,
                        c -> c in ('CON016', 'CON018')
            ) as exp_consegnato_recapitista,
            filter (
                        event_list_codes,
                        c -> c in ('CON080')
            ) as exp_atto_stampato,
            filter (
                        event_list_codes,
                        c -> c like 'CON0%'
            ) as exp_lavorazioni_consolidatore,
            array_join(
                    array_distinct(
                            transform(
                                        event_list,
                                        e -> e.paperProg_deliveryFailureCause
                            )
                    ), ' ') as exp_delivery_failure_cause,
            regexp_extract(requestId, 'pn-cons-000~.*ATTEMPT_(.)\\.PCRETRY_.', 1) as attempt,
            *
        FROM
            exported_data_reloaded
    ),
    ecmetadata_categorization as (
        select
            (CASE
                 when size(exp_scarti_consolidatore)	> 0 then 'exp_scarti_consolidatore'
                 when size(exp_atto_consegnato_esito) > 0 then 'exp_finali_positivi'
                 when size(exp_atto_non_consegnato_esito) > 0 AND attempt = 0 AND (exp_delivery_failure_cause like '%M02%' OR exp_delivery_failure_cause like '%M05%') then 'exp_finali_mancataconsegna_no2t_tent1'
                 when size(exp_giacenza_esito) > 0 then 'exp_finali_giacenza'
                 when (size(exp_atto_non_consegnato_esito) > 0 OR size(exp_atto_non_consegnato_irrep_esito) > 0) AND attempt = 0 then 'exp_finali_irreperibile_tent1'
                 when (size(exp_atto_non_consegnato_esito) > 0 OR size(exp_atto_non_consegnato_irrep_esito) > 0) AND attempt = 1 then 'exp_finali_irreberibile_tent2'
                 when size(exp_atto_consegnato_pre_esito) > 0 AND attempt = 0 then 'exp_preesito_positivi_tent1'
                 when size(exp_atto_consegnato_pre_esito) > 0 AND attempt = 1 then 'exp_preesito_positivi_tent2'
                 when size(exp_atto_non_consegnato_pre_esito) > 0 AND attempt = 0 AND (exp_delivery_failure_cause like '%M02%' OR exp_delivery_failure_cause like '%M05%') then 'exp_preesito_mancataconsegna_no2t_tent1'
                 when size(exp_giacenza_pre_esito) > 0 then 'exp_preesito_giacenza'
                 when (size(exp_atto_non_consegnato_pre_esito) > 0 OR SIZE(exp_atto_non_consegnato_irrep_pre_esito) >0 ) AND attempt = 0 then 'exp_preesito_irreperibile_tent1'
                 when (size(exp_atto_non_consegnato_pre_esito) > 0 OR SIZE(exp_atto_non_consegnato_irrep_pre_esito) >0 ) AND attempt = 1 then 'exp_preesito_irreperibile_tent2'
                 when size(exp_inesito) > 0 AND attempt = 0 then 'exp_inesito_tent1'
                 when size(exp_inesito) > 0 AND attempt = 1 then 'exp_inesito_tent2'
                 when size(exp_avvio_internazionale) > 0 AND attempt = 0 then 'exp_internazionale_tent1'
                 when size(exp_avvio_internazionale) > 0 AND attempt = 1 then 'exp_internazionale_tent2'
                 when size(exp_errori_pre_esito) > 0 then 'exp_bloccato_su_furto'
                 when size(exp_consegnato_recapitista) > 0 AND attempt = 0 then 'exp_consegnato_recapito_tent1'
                 when size(exp_consegnato_recapitista) > 0 AND attempt = 1 then 'exp_consegnato_recapito_tent2'
                 when size(exp_atto_stampato) > 0 AND attempt = 0 then 'exp_spedizione_stampata_tent1'
                 when size(exp_atto_stampato) > 0 AND attempt = 1 then 'exp_spedizione_stampata_tent2'
                 when size(exp_lavorazioni_consolidatore) > 0 AND attempt = 0 then 'exp_spedizione_in_lavorazione_consolidatore_tent1'
                 when size(exp_lavorazioni_consolidatore) > 0 AND attempt = 1 then 'exp_spedizione_in_lavorazione_consolidatore_tent2'
                END) as migliore_evento_trovato,
            rank() OVER (PARTITION BY iun ORDER BY requestId) AS requestId_order,
            count(1) OVER (PARTITION BY iun ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS requests_for_iun,
            *
        from
            ecmetadata_enriched
    ), final_details AS (
    select
        migliore_evento_trovato,
        requests_for_iun,
        exp_scarti_consolidatore,
        exp_atto_consegnato_pre_esito,
        exp_atto_non_consegnato_pre_esito,
        exp_giacenza_pre_esito,
        exp_inesito,
        exp_avvio_internazionale,
        exp_errori_pre_esito,
        exp_atto_consegnato_esito,
        exp_atto_non_consegnato_esito,
        exp_giacenza_esito,
        exp_atto_stampato,
        exp_lavorazioni_consolidatore,
        exp_delivery_failure_cause,
        attempt,
        modalita_consegna,
        livello_di_perfezionamento,
        sent_at_day,
        description,
        iun,
        missing_file_keys,
        send_analog_timelineid,
        regexp_extract(requestId, 'pn-cons-000~(.*)', 1) AS requestId,
        requestTimestamp,
        clientRequestTimeStamp,
        statusRequest,
        `version`,
        event_list_length,
        dynamoExportName
    from ecmetadata_categorization
    where
        requests_for_iun == requestId_order AND
        livello_di_perfezionamento = 'NE_DECORRENZA_NE_VISUALIZZATO'
) SELECT
    *
FROM final_details;