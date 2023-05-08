--------------------------------------------------------------- ASIG. DIM. OPERACIONAL ---------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Asignación de agrupador contable
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
CREATE TABLE MIS_TMP_STG_${var:tabla}_1 AS 
SELECT a.*, CASE WHEN a.COD_GL IS NOT NULL THEN a.COD_GL ELSE b.COD_GL_GROUP END AS COD_GL_GROUP/*, c.DES_GL_GROUP, c.ACCOUNT_CONCEPT*/
FROM (
    SELECT a.* 
    FROM MIS_APR_${var:tabla}_M a
    WHERE a.DATA_DATE = '${var:periodo}') a
LEFT JOIN MIS_PAR_REL_CAF_OPER b    
ON a.COD_ENTITY = b.COD_ENTITY AND a.COD_PRODUCT = b.COD_PRODUCT AND a.COD_SUBPRODUCT = b.COD_SUBPRODUCT AND 
   a.COD_CURRENCY = b.COD_CURRENCY AND a.COD_VALUE = b.COD_VALUE AND a.COD_BLCE_STATUS = b.COD_BLCE_STATUS 
--LEFT JOIN MIS_PAR_CAT_CAF c 
--ON b.COD_GL_GROUP=c.COD_GL_GROUP
;  
    
----Rechazo de registros sin agrupador contable
ALTER TABLE MIS_REJECTIONS_DIMENSIONS_M DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'AGRUPADOR CONTABLE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', 'N/A', 'N/A', a.COD_PRODUCT, a.COD_SUBPRODUCT, 'N/A', a.COD_BLCE_STATUS, a.COD_VALUE, 
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_1 a 
WHERE a.COD_GL_GROUP IS NULL
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_VALUE, a.COD_ENTITY, a.COD_CURRENCY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_BLCE_STATUS
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0; --Unicamente se guardan rechazos con saldos diferente de cero


----Asignación de cuenta P&G
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
CREATE TABLE MIS_TMP_STG_${var:tabla}_2 AS 
SELECT a.*, CASE WHEN b.IND_DETAIL = 'Y' THEN CONCAT(b.COD_PL_ACC,'/',ISNULL(ISNULL(e.COD_BLCE_PROD,f.COD_BLCE_PROD),'')) ELSE b.COD_PL_ACC END AS COD_PL_ACC, c.DES_PL_ACC 
FROM (
    SELECT a.*
    FROM MIS_TMP_STG_${var:tabla}_1 a
    WHERE a.COD_GL_GROUP IS NOT NULL) a 
LEFT JOIN MIS_PAR_REL_PL_ACC b 
ON a.COD_ENTITY=b.COD_ENTITY AND a.COD_GL_GROUP=b.COD_GL_GROUP AND a.COD_CURRENCY=b.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_PL_ACC c
ON b.COD_PL_ACC = c.COD_PL_ACC
LEFT JOIN MIS_APR_CONTRACT_DT d
ON a.DATA_DATE = d.DATA_DATE AND a.IDF_CTO = d.IDF_CTO AND a.COD_ENTITY = d.COD_ENTITY 
LEFT JOIN mis_par_rel_bp_oper e
ON a.COD_ENTITY=e.COD_ENTITY AND a.COD_GL_GROUP=e.COD_GL_GROUP AND a.COD_CURRENCY=e.COD_CURRENCY AND a.COD_PRODUCT=e.COD_PRODUCT AND a.COD_SUBPRODUCT=e.COD_SUBPRODUCT 
AND IF(a.COD_ACT_TYPE IS NULL OR TRIM(a.COD_ACT_TYPE) = '', 'NA', a.COD_ACT_TYPE)=IF(e.COD_ACT_TYPE IS NULL OR TRIM(e.COD_ACT_TYPE) = '', 'NA', e.COD_ACT_TYPE) 
AND IF(d.COD_RATE_TYPE IS NULL OR TRIM(d.COD_RATE_TYPE) = '', 'NA', d.COD_RATE_TYPE) = IF(e.COD_RATE_TYPE IS NULL OR TRIM(e.COD_RATE_TYPE) = '', 'NA', e.COD_RATE_TYPE)
LEFT JOIN MIS_PAR_REL_BP_OPER f
ON a.COD_ENTITY=f.COD_ENTITY AND a.COD_GL_GROUP=f.COD_GL_GROUP AND a.COD_CURRENCY=f.COD_CURRENCY AND a.COD_PRODUCT=f.COD_PRODUCT AND a.COD_SUBPRODUCT=f.COD_SUBPRODUCT
AND IF(d.COD_RATE_TYPE IS NULL OR TRIM(d.COD_RATE_TYPE) = '', 'NA', d.COD_RATE_TYPE) = IF(f.COD_RATE_TYPE IS NULL OR TRIM(f.COD_RATE_TYPE) = '', 'NA', f.COD_RATE_TYPE)
AND TRIM(f.cod_act_type) = '9999'; 

----Rechazo de registros sin cuenta P&G
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'CUENTA DE GESTION' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A',  
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_2 a 
WHERE a.COD_PL_ACC IS NULL
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;


----Asignación de producto de balance
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
CREATE TABLE MIS_TMP_STG_${var:tabla}_3 AS
SELECT a.*, b.COD_RATE_TYPE, CASE WHEN c.COD_BLCE_PROD IS NOT NULL THEN c.COD_BLCE_PROD WHEN d.COD_BLCE_PROD IS NOT NULL THEN d.COD_BLCE_PROD END AS COD_BLCE_PROD, e.DES_BLCE_PROD
FROM (
    SELECT a.*
    FROM MIS_TMP_STG_${var:tabla}_2 a
    WHERE a.COD_PL_ACC IS NOT NULL) a
LEFT JOIN MIS_APR_CONTRACT_DT b
ON a.DATA_DATE = b.DATA_DATE AND a.IDF_CTO = b.IDF_CTO AND a.COD_ENTITY = b.COD_ENTITY 
LEFT JOIN MIS_PAR_REL_BP_OPER c
ON a.COD_ENTITY=c.COD_ENTITY AND a.COD_GL_GROUP=c.COD_GL_GROUP
    AND a.COD_PRODUCT=c.COD_PRODUCT AND a.COD_SUBPRODUCT=c.COD_SUBPRODUCT 
    AND IF(a.COD_ACT_TYPE IS NULL OR TRIM(a.COD_ACT_TYPE) = '', 'NA', a.COD_ACT_TYPE) = IF(c.COD_ACT_TYPE IS NULL OR TRIM(c.COD_ACT_TYPE) = '', 'NA', c.COD_ACT_TYPE) AND IF(b.COD_RATE_TYPE IS NULL OR TRIM(b.COD_RATE_TYPE) = '', 'NA', b.COD_RATE_TYPE) = IF(c.COD_RATE_TYPE IS NULL OR TRIM(c.COD_RATE_TYPE) = '', 'NA', c.COD_RATE_TYPE)
LEFT JOIN MIS_PAR_REL_BP_OPER d
ON a.COD_ENTITY=d.COD_ENTITY AND a.COD_GL_GROUP=d.COD_GL_GROUP AND a.COD_CURRENCY=d.COD_CURRENCY 
    AND a.COD_PRODUCT=d.COD_PRODUCT AND a.COD_SUBPRODUCT=d.COD_SUBPRODUCT
    AND IF(b.COD_RATE_TYPE IS NULL OR TRIM(b.COD_RATE_TYPE) = '', 'NA', b.COD_RATE_TYPE) = IF(d.COD_RATE_TYPE IS NULL OR TRIM(d.COD_RATE_TYPE) = '', 'NA', d.COD_RATE_TYPE)
AND TRIM(d.cod_act_type) = '9999'
LEFT JOIN MIS_PAR_CAT_BLCE_PROD e
ON c.COD_BLCE_PROD = e.COD_BLCE_PROD;

----Rechazo de registros sin producto de balance
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'PRODUCTO DE BALANCE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, a.COD_ACT_TYPE, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_RATE_TYPE, 'N/A', 'N/A',   
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30, 10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30, 10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_3 a  
WHERE a.COD_BLCE_PROD IS NULL
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, a.COD_RATE_TYPE
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;


----Asignación de línea de negocio
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4; 
CREATE TABLE MIS_TMP_STG_${var:tabla}_4 AS 
SELECT a.*, d.COD_BUSINESS_LINE, e.DES_BUSINESS_LINE, b.IDF_CLI, c.COD_MANAGER, c.TYP_DOC_ID, c.COD_COUNTRY, c.COD_SECTOR, c.COD_SEGMENT, c.COD_SUBSEGMENT, c.COD_TYP_CLNT
FROM (
    SELECT a.*
    FROM MIS_TMP_STG_${var:tabla}_3 a
    WHERE a.COD_BLCE_PROD IS NOT NULL) a 
LEFT JOIN MIS_APR_CONTRACT_DT b
ON a.DATA_DATE = b.DATA_DATE AND a.IDF_CTO = b.IDF_CTO AND a.COD_ENTITY = b.COD_ENTITY 
LEFT JOIN MIS_APR_CLIENT_DT c
ON b.DATA_DATE = c.DATA_DATE AND b.IDF_CLI = c.IDF_CLI 
LEFT JOIN MIS_PAR_REL_BL_OPER d 
ON c.COD_TYP_CLNT = d.COD_TYP_CLNT AND 
    IF(d.COD_BLCE_PROD IS NULL OR d.COD_BLCE_PROD = '', 'NA', a.COD_BLCE_PROD) = IF(d.COD_BLCE_PROD IS NULL OR d.COD_BLCE_PROD = '', 'NA', d.COD_BLCE_PROD) --a.COD_BLCE_PROD = d.COD_BLCE_PROD 
LEFT JOIN MIS_PAR_CAT_BL e 
ON d.COD_BUSINESS_LINE = e.COD_BUSINESS_LINE;

----Rechazo de registros sin línea de negocio     
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'LINEA DE NEGOCIO' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    a.COD_BLCE_PROD, 'N/A', a.COD_TYP_CLNT, 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NULL 
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_BLCE_PROD, a.COD_TYP_CLNT
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;

----Rechazo de registros sin sector   
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'SECTOR' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', a.COD_MANAGER, 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NOT NULL AND a.COD_SECTOR IS NULL 
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_MANAGER
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;

----Rechazo de registros sin segmento   
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'SEGMENTO' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', a.COD_MANAGER, a.TYP_DOC_ID, 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL,  a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NOT NULL AND a.COD_SECTOR IS NOT NULL AND a.COD_SEGMENT IS NULL 
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_MANAGER, a.TYP_DOC_ID
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;
/*
----Rechazo de registros sin subsegmento   
INSERT INTO MIS_REJECTIONS_DIMENSIONS_M (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'SUBSEGMENTO' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', a.COD_MANAGER, 'N/A', a.COD_SEGMENT, CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NOT NULL AND a.COD_SECTOR IS NOT NULL AND a.COD_SEGMENT IS NOT NULL AND a.COD_SUBSEGMENT IS NULL 
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_MANAGER, a.COD_SEGMENT, a.COD_COUNTRY
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;
*/

----Inserción de registros en la tabla de dimensiones DWH
ALTER TABLE MIS_DWH_${var:tabla}_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla}_M PARTITION (DATA_DATE, COD_INFO_SOURCE)
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, 
    a.COD_ACT_TYPE, a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_GL_GROUP, c.DES_GL_GROUP, c.ACCOUNT_CONCEPT,  
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, NULL, NULL, NULL, NULL, NULL, NULL, a.DATA_DATE, a.COD_INFO_SOURCE 
FROM MIS_TMP_STG_${var:tabla}_4 a
LEFT JOIN MIS_PAR_CAT_CAF c 
ON a.COD_GL_GROUP=c.COD_GL_GROUP AND a.COD_ENTITY = c.COD_ENTITY AND a.COD_CURRENCY=c.COD_CURRENCY
WHERE a.COD_BUSINESS_LINE IS NOT NULL AND a.COD_SECTOR IS NOT NULL AND a.COD_SEGMENT IS NOT NULL;


--Eliminación de tablas Temporales 
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;