------------ Proceso de registro de ejecucion------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

/* 
DROP TABLE IF EXISTS MIS_TMP_CALENDAR_END_D;
CREATE TABLE MIS_TMP_CALENDAR_END_D AS
SELECT * FROM MIS_PAR_CALENDAR
;

TRUNCATE TABLE MIS_PAR_CALENDAR;

INSERT INTO MIS_PAR_CALENDAR (FREQUENCY, DATA_DATE, STATUS, EXEC_START_DATE, EXEC_END_DATE)
SELECT FREQUENCY, DATA_DATE, CASE WHEN FREQUENCY = 'D' AND DATA_DATE = '${var:periodo}' THEN 'F' else STATUS end, EXEC_START_DATE, 
CASE WHEN FREQUENCY = 'D' AND DATA_DATE = '${var:periodo}' THEN now() else EXEC_END_DATE end EXEC_END_DATE
FROM MIS_TMP_CALENDAR_END_D;
*/
/*
ALTER TABLE MIS_PAR_CALENDAR
DROP IF EXISTS PARTITION (FREQUENCY = 'D', DATA_DATE = '${var:periodo}', ACTUAL_DATE = STRLEFT(CAST(NOW() AS STRING),10))
;

INSERT INTO MIS_PAR_CALENDAR
PARTITION (FREQUENCY, DATA_DATE, ACTUAL_DATE)
SELECT  'S', NOW(), NOW(), 'D', '${var:periodo}', STRLEFT(CAST(NOW() AS STRING),10)
;
*/

TRUNCATE TABLE MIS_TMP_CALENDAR_END_D;

INSERT INTO  MIS_TMP_CALENDAR_END_D 
SELECT * from MIS_PAR_CALENDAR
WHERE DATA_DATE = '${var:periodo}' AND FREQUENCY = 'D' AND ACTUAL_DATE = STRLEFT(CAST(NOW() AS STRING),10);

ALTER TABLE MIS_PAR_CALENDAR
DROP IF EXISTS PARTITION (FREQUENCY = 'D', DATA_DATE = '${var:periodo}', ACTUAL_DATE = STRLEFT(CAST(NOW() AS STRING),10));

INSERT INTO MIS_PAR_CALENDAR 
( STATUS, EXEC_START_DATE, EXEC_END_DATE, FREQUENCY, DATA_DATE, ACTUAL_DATE)
SELECT 
CASE WHEN FREQUENCY = 'D' AND DATA_DATE = '${var:periodo}' THEN 'S' ELSE STATUS END, EXEC_START_DATE, 
CASE WHEN FREQUENCY = 'D' AND DATA_DATE = '${var:periodo}' THEN now() ELSE EXEC_END_DATE END EXEC_END_DATE, FREQUENCY, DATA_DATE, ACTUAL_DATE

FROM MIS_TMP_CALENDAR_END_D;