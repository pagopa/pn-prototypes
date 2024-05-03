/**
DEPENDENCIES:
  - /source-views/pnTimelines.sql
*/

create or REPLACE temporary view analog_process AS
WITH analog_notification_timeline AS (
SELECT
	iun,
	paid,
	CASE category
	  WHEN 'SCHEDULE_ANALOG_WORKFLOW' THEN `timestamp`
	  ELSE '1000-01-01T00:21:53.716841422Z'
	END
		as analog_schedule_timestamp,
	CASE category
	  WHEN 'SCHEDULE_ANALOG_WORKFLOW' THEN 1
	  ELSE 0
	END
		as analog_schedule_count,
	CASE category
	  WHEN 'ANALOG_SUCCESS_WORKFLOW' THEN `timestamp`
	  ELSE '1000-01-01T00:21:53.716841422Z'
	END
		as analog_success_timestamp,
	CASE category
	  WHEN 'ANALOG_SUCCESS_WORKFLOW' THEN 1
	  ELSE 0
	END
		as analog_success_count,
	CASE category
	  WHEN 'ANALOG_FAILURE_WORKFLOW' THEN `timestamp`
	  ELSE '1000-01-01T00:21:53.716841422Z'
	END
		as analog_failure_timestamp,
	CASE category
	  WHEN 'ANALOG_FAILURE_WORKFLOW' THEN 1
	  ELSE 0
	END
		as analog_failure_count,
	CASE category
	  WHEN 'NOTIFICATION_VIEWED' THEN `timestamp`
	  ELSE '1000-01-01T00:21:53.716841422Z'
	END
		as viewed_timestamp,
	CASE category
	  WHEN 'REFINEMENT' THEN 1
	  ELSE 0
	END
		as refinement_count,
	CASE category
	  WHEN 'NOTIFICATION_VIEWED' THEN 1
	  ELSE 0
	END
		as viewed_count,
	CASE category
	  WHEN 'NOTIFICATION_CANCELLED' THEN 1
	  ELSE 0
	END
		as cancelled_count
FROM
  incremental_timeline
WHERE
    category in ('SCHEDULE_ANALOG_WORKFLOW','ANALOG_SUCCESS_WORKFLOW','ANALOG_FAILURE_WORKFLOW', 'NOTIFICATION_VIEWED', 'REFINEMENT', 'NOTIFICATION_CANCELLED')
), analog_notification_total AS (
	SELECT
		iun,
		paid,
		max(analog_schedule_timestamp) as analog_schedule_timestamp,
		sum(analog_schedule_count) as analog_schedule_count,
		max(analog_success_timestamp) as analog_success_timestamp,
		sum(analog_success_count) as analog_success_count,
		max(analog_failure_timestamp) as analog_failure_timestamp,
		sum(analog_failure_count) as analog_failure_count,
		max(viewed_timestamp) as viewed_timestamp,
		sum(refinement_count) as refinement_count,
		sum(viewed_count) as viewed_count,
		sum(cancelled_count) as cancelled_count
	FROM analog_notification_timeline
	GROUP BY iun, paid
), analog_notification_progress AS (
	SELECT *
	FROM analog_notification_total
	WHERE analog_schedule_count = 1 AND
		NOT (analog_success_count = 1 OR analog_failure_count = 1)
), prepare_analog_domicile_timeline AS (
	SELECT iun, paid, notificationSentAt, timelineElementId
	FROM incremental_timeline
	WHERE category = 'PREPARE_ANALOG_DOMICILE'
), send_analog_domicile_timeline AS (
	SELECT *
	FROM incremental_timeline
	WHERE category = 'SEND_ANALOG_DOMICILE'
), send_analog_feedback_timeline AS (
	SELECT *
	FROM incremental_timeline
	WHERE category ='SEND_ANALOG_FEEDBACK'
), send_analog_domicile4analog_notification_progress AS (
	SELECT
		 a.iun,
		 a.paid,
		 a.notificationSentAt,
		 get_json_object( t.details, '$.productType.S' ) as productType,
		 get_json_object( t.details, '$.recIndex.N' ) as recIdx,
		 a.timelineElementId as prepareRequestId,
		 get_json_object( t.details, '$.physicalAddress.M.zip.S' ) as destZip,
		 get_json_object( t.details, '$.physicalAddress.M.foreignState.S' ) as destForeignState,
		 get_json_object( t.details, '$.numberOfPages.N' ) as numberOfPages,
		 get_json_object( t.details, '$.envelopeWeight.N' ) as envelopeWeight,
		 if(t.iun is null, 0, 1) as sent2consolidatore,
		 refinement_count as refinement,
		 viewed_count as viewed,
		 cancelled_count as cancelled,
		 t.timelineelementid as sendRequestId
	FROM
		prepare_analog_domicile_timeline a LEFT JOIN send_analog_domicile_timeline t
		ON a.timelineElementId = get_json_object( t.details, '$.prepareRequestId.S' )
		JOIN analog_notification_progress p
		ON a.iun = p.iun
), send_analog_feedback4analog_notification_progress AS (
	SELECT
		 p.iun,
		 t.timelineelementid,
		 get_json_object( t.details, '$.registeredLetterCode.S' ) as registeredLetterCode,
		 get_json_object( t.details, '$.deliveryDetailCode.S' ) as   deliveryDetailCode,
		 get_json_object( t.details, '$.sendRequestId.S' ) as  sendRequestId
	FROM
		send_analog_feedback_timeline t JOIN analog_notification_progress p
		ON p.iun = t.iun
), shipment_match AS (
	SELECT
		s.iun,
		s.paid,
		s.notificationSentAt,
		s.productType,
		s.prepareRequestId,
		s.sendRequestId,
	    s.destZip,
		s.destForeignState,
		s.numberOfPages,
		s.envelopeWeight,
		sent2consolidatore,
		p.timelineelementid as feedbackid,
		refinement,
		viewed,
		cancelled
	FROM
		send_analog_domicile4analog_notification_progress s LEFT JOIN send_analog_feedback4analog_notification_progress p
		ON s.sendRequestId = p.sendRequestId
), shipment_match_filtering AS (
  SELECT s.iun,
		s.paid,
		s.notificationsentat,
		split_part(s.notificationsentat,'-',1) as `year`,
		split_part(s.notificationsentat,'-',2) as `month`,
		split_part(split_part(s.notificationsentat,'-',3),'T',1) as `date`,
		s.prepareRequestId,
		s.sendRequestId,
		s.feedbackid,
		s.productType,
	    s.destZip,
		s.destForeignState,
		sent2consolidatore,
		s.viewed,
		refinement,
		cancelled
FROM shipment_match s
WHERE split_part(s.notificationsentat,'-',1) ="2023"
AND paid != '4a4149af-172e-4950-9cc8-63ccc9a6d865'
AND s.feedbackid is NULL
and refinement =0 and cancelled = 0 AND viewed = 0
)
SELECT *
FROM shipment_match_filtering;