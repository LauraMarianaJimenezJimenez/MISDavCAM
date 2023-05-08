------------ Actualizacion de saldos medios en dwh ------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

TRUNCATE TABLE IF EXISTS MIS_DWH_${var:tabla}_TMP_LOAD_M;
DROP TABLE IF EXISTS MIS_DWH_${var:tabla}_TMP_LOAD_M;
CREATE TABLE MIS_DWH_${var:tabla}_TMP_LOAD_M AS
SELECT * FROM MIS_DWH_${var:tabla}_M
WHERE DATA_DATE = '${var:periodo}';

ALTER TABLE MIS_DWH_${var:tabla}_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla}_M PARTITION(DATA_DATE,COD_INFO_SOURCE)
SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, 
    a.COD_BLCE_STATUS, a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, 
    a.EOPBAL_CAP, a.EOPBAL_INT, 
    IF(B.COD_VALUE <> 'INT',ISNULL(B.AVGBAL_CAP,0),0) AS AVGBAL_CAP, 
    IF(B.COD_VALUE = 'INT',ISNULL(B.AVGBAL_CAP,0),0) AS AVGBAL_INT, 
    a.PL, 
    a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, 
    a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, 
    NULL, NULL, NULL, NULL, NULL, NULL, a.DATA_DATE, a.COD_INFO_SOURCE
FROM MIS_DWH_${var:tabla}_TMP_LOAD_M AS A
LEFT JOIN MIS_LOAD_AVGBAL_ASSETS_M AS B  
ON (A.IDF_CTO = B.IDF_CTO AND A.COD_BLCE_STATUS = B.COD_BLCE_STATUS AND A.COD_VALUE = B.COD_VALUE)  
WHERE (A.DATA_DATE = '${var:periodo}');


------------ Limpiar las Tablas Temporales ------------

TRUNCATE TABLE MIS_DWH_${var:tabla}_TMP_LOAD_M;
DROP TABLE MIS_DWH_${var:tabla}_TMP_LOAD_M;