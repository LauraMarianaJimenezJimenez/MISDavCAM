--------------------------------------------------------------- MIS_APR_MOV ------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};

--- Limpieza de partición tablas apr mov----
ALTER TABLE MIS_APR_MV
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_APR_MV
PARTITION (DATA_DATE='${var:periodo}')
SELECT *, 'MV_A1' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_A1
UNION ALL
SELECT *, 'MV_A2' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_A2
UNION ALL
SELECT *, 'MV_BANX' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_BANX
UNION ALL
SELECT *, 'MV_MC' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_MC
UNION ALL
SELECT *, 'MV_ONE' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_ONE
UNION ALL
SELECT *, 'MV_DEBT' AS FUENTE_ORIGEN FROM MIS_LOAD_MV_DEBT;