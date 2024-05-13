/*
$QueryMetadata
{
    "name": "pivotedEvents",
    "dependencies": [
        {
            "name": "rankedEvents",
            "location": "analog-delivery-monitoring/logical-views/rankedEvents.sql"
        }
    ]
}
*/
create or replace temporary view pivotedEvents as
	SELECT * FROM rankedEvents
	PIVOT
	(
		count(status)
		for status in (
			'CON016',
			'RECRN001A', 'RECRN002A', 'RECRN003A', 'RECRN004A', 'RECRN005A', 'RECRN001C', 'RECRN002C', 'RECRN003C', 'RECRN004C', 'RECRN005C', 'RECRN002D', 'RECRN002F', 'RECRN006', 'RECRN010', 'RECRN011', 'RECRN013',
			'RECAG001A', 'RECAG002A', 'RECAG003A', 'RECAG005A', 'RECAG006A', 'RECAG007A', 'RECAG008A', 'RECAG001C', 'RECAG002C', 'RECAG003C', 'RECAG005C', 'RECAG006C', 'RECAG007C', 'RECAG008C', 'RECAG003D', 'RECAG003F', 'RECAG004', 'RECAG010', 'RECAG011A', 'RECAG013',
			'RECRS002A', 'RECRS004A', 'RECRS005A', 'RECRS001C', 'RECRS002C', 'RECRS003C', 'RECRS004C', 'RECRS005C', 'RECRS002D', 'RECRS002F', 'RECRS006', 'RECRS010', 'RECRS011', 'RECRS013',
			'RECRI003A', 'RECRI004A', 'RECRI003C', 'RECRI004C', 'RECRI005',
			'RECRSI003C', 'RECRSI004A', 'RECRSI004C', 'RECRSI005'
		)
	)
;