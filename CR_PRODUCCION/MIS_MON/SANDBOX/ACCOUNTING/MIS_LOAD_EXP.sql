--------------------------------------------------------------- MIS_APR_ACCOUNTING_M ---------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_CARGA_BASE; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/BASE_GASTO.CSV' INTO TABLE MIS_LOAD_CARGA_BASE;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_EXPENSES_M 
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Aprovisionamiento de la informacion de Resultados ---
INSERT INTO MIS_APR_EXPENSES_M
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'EXP' AS COD_CONT, RPAD(TRIM(a.CUENTA), 16, '0') AS COD_GL, TRIM(a.NOMBRE_CUENTA) AS DES_GL, 
    TRIM(a.CC) AS COD_ACCO_CENT, TRIM(a.COD) AS COD_EXPENSE, NULL AS COD_OFFI, TRIM(a.COD) AS COD_NAR, NULL AS COD_BLCE_STATUS, 'CRC' AS COD_CURRENCY, TRIM(a.CIA) AS COD_ENTITY,
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    CAST(a.IMPORTE_MES AS decimal(30, 10)) AS PL, 'CARGA_BASE' AS COD_INFO_SOURCE
FROM MIS_LOAD_CARGA_BASE a 
WHERE TRIM(a.CIA) != '99' AND STRLEFT(TRIM(a.CUENTA), 1) IN ('4', '5');