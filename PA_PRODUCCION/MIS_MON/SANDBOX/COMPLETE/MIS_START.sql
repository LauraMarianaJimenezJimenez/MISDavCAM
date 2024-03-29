------------ Proceso de registro de ejecucion------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;
/*
DROP TABLE IF EXISTS MIS_TMP_CALENDAR_INI_D;
CREATE TABLE MIS_TMP_CALENDAR_INI_D AS
SELECT * FROM MIS_PAR_CALENDAR
WHERE DATA_DATE <> '${var:periodo}'
AND FREQUENCY <> 'D'
;

TRUNCATE TABLE MIS_PAR_CALENDAR;

INSERT INTO MIS_PAR_CALENDAR (FREQUENCY, DATA_DATE, STATUS, EXEC_START_DATE, EXEC_END_DATE)
SELECT 'M', '${var:periodo}', 'E', NOW(), NULL
UNION ALL
SELECT FREQUENCY, DATA_DATE, STATUS, EXEC_START_DATE, EXEC_END_DATE
FROM MIS_TMP_CALENDAR_INI_D;
*/

ALTER TABLE MIS_PAR_CALENDAR
DROP IF EXISTS PARTITION (FREQUENCY = 'M', DATA_DATE = '${var:periodo}', ACTUAL_DATE = STRLEFT(CAST(NOW() AS STRING),10))
;

INSERT INTO MIS_PAR_CALENDAR
PARTITION (FREQUENCY, DATA_DATE, ACTUAL_DATE)
SELECT  'F', NOW(), NULL, 'M', '${var:periodo}', STRLEFT(CAST(NOW() AS STRING),10)
;