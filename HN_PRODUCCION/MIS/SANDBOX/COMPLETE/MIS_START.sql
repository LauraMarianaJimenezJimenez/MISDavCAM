------------ Proceso de registro de ejecucion------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;



--- Limpieza de partición ----
-- ALTER TABLE MIS_PAR_CALENDAR
-- DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', frequency ='D');

-- INSERT INTO MIS_PAR_CALENDAR (STATUS, EXEC_START_DATE, EXEC_END_DATE, DATA_DATE, FREQUENCY)
-- SELECT 'E', NOW(), NULL, '${var:periodo}', 'D'
-- ; 


ALTER TABLE MIS_PAR_CALENDAR
DROP IF EXISTS PARTITION (FREQUENCY = 'D', DATA_DATE = '${var:periodo}', ACTUAL_DATE = STRLEFT(CAST(NOW() AS STRING),10))
;

INSERT INTO MIS_PAR_CALENDAR
PARTITION (FREQUENCY, DATA_DATE, ACTUAL_DATE)
SELECT  'F', NOW(), NULL, 'D', '${var:periodo}', STRLEFT(CAST(NOW() AS STRING),10)
;