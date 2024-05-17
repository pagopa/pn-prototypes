with
    ecmetadata_enriched as (
        select
            filter (
                        event_list_codes,
                        c -> c in ('RECAG001A','RECAG001B','RECRN001A','RECRN001B','RECRN003A','RECRN003B','RECRN005A','RECRN005B','RECAG002A','RECAG002B','RECAG005A','RECAG005B','RECRI003A','RECRI003B')
            ) as pre_esiti_positivi,
            filter (
                        event_list_codes,
                        c -> c in ('RECAG001C','RECRN001C','RECRN003C','RECRN005C','RECAG002C','RECAG005C','RECRI003C')
            ) as esiti_finali_positivi,
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
                        c -> c in ('RECRS002A','RECRS002D','RECRN002A','RECRN002D','RECAG003A','RECAG003D', 'RECRSI004A', 'RECRI004A')
            ) as exp_atto_non_consegnato_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS003C','RECRS004A','RECRS005A','RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECAG006A','RECAG007A','RECAG008A')
            ) as exp_giacenza_pre_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS010', 'RECRN010', 'RECAG010','RECRS011', 'RECRN011', 'RECAG011A', 'RECRSI001', 'RECRI001', 'RECRSI002', 'RECRI002')
            ) as exp_aggiornamenti_tecnici_recapito,
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
                        c -> c in ('RECRS002C','RECRS002F','RECRN002C','RECRN002F','RECAG003C','RECAG003F','RECRSI004C','RECRI004C')
            ) as exp_atto_non_consegnato_esito,
            filter (
                        event_list_codes,
                        c -> c in ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            ) as exp_giacenza_esito,
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
                 when size(exp_atto_non_consegnato_esito) > 0 AND attempt = 0 then 'exp_finali_irreperibile_tent1'
                 when size(exp_atto_non_consegnato_esito) > 0 AND attempt = 1 then 'exp_finali_irreberibile_tent2'
                 when size(exp_atto_consegnato_pre_esito) > 0 then 'exp_preesito_positivi'
                 when size(exp_atto_non_consegnato_pre_esito) > 0 AND attempt = 0 AND (exp_delivery_failure_cause like '%M02%' OR exp_delivery_failure_cause like '%M05%') then 'exp_preesito_mancataconsegna_no2t_tent1'
                 when size(exp_giacenza_pre_esito) > 0 then 'exp_preesito_giacenza'
                 when size(exp_atto_non_consegnato_pre_esito) > 0 AND attempt = 0 then 'exp_preesito_irreperibile_tent1'
                 when size(exp_atto_non_consegnato_pre_esito) > 0 AND attempt = 1 then 'exp_preesito_irreperibile_tent2'
                 when size(exp_aggiornamenti_tecnici_recapito) > 0 AND attempt = 0 then 'exp_aggiornamenti_giacenza_trasportointernazionale_tent1'
                 when size(exp_aggiornamenti_tecnici_recapito) > 0 AND attempt = 1 then 'exp_aggiornamenti_giacenza_trasportointernazionale_tent2'
                 when size(exp_errori_pre_esito) > 0 then 'exp_bloccato_su_furto'
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
    )
select
    *
from ecmetadata_categorization
where
    requests_for_iun == requestId_order AND
    livello_di_perfezionamento = 'NE_DECORRENZA_NE_VISUALIZZATO'
  AND migliore_evento_trovato = 'exp_preesito_giacenza'
;