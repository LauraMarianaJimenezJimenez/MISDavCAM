--------------------------------------------------------------- ASIG. DIM. OPERACIONAL ----------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--COMPUTE INCREMENTAL STATS MIS_APR_${var:tabla} PARTITION (DATA_DATE = '${var:periodo}');

--COD_CONT EN RECHAZOS

------------ Asignación del Agrupador Contable ------------
--1. Asignar agrupador contable a operaciones que no tienen cuenta contable
-----a. Usando producto balance y cuenta contable (CAF)
-----b. Usando únicamnete producto balance (CAF_PR)
--2. Asignar agrupador contable a operaciones que tienen cuenta contable que pertenece a un agrupador especial (ACC)
--3. Asignar agrupador contable a operaciones que tienen cuenta contable que no pertenecen a un agrupador específico (APR)
-----------------------------------------------------------
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
CREATE TABLE MIS_TMP_STG_${var:tabla}_1 AS
SELECT APR.*, --CAF.COD_GL_GROUP, CAT.DES_GL_GROUP, CAT.ACCOUNT_CONCEPT 
CASE WHEN IFNULL(APR.COD_GL, '') = '' AND CAF.COD_GL_GROUP IS NOT NULL THEN CAF.COD_GL_GROUP
     WHEN IFNULL(APR.COD_GL, '') = '' AND CAF_PR.COD_GL_GROUP IS NOT NULL THEN CAF_PR.COD_GL_GROUP
     WHEN APR.COD_GL != ACC.COD_GL_GROUP AND ACC.COD_GL_GROUP IS NOT NULL THEN ACC.COD_GL_GROUP
     ELSE APR.COD_GL END AS COD_GL_GROUP,
CASE WHEN IFNULL(APR.COD_GL, '') = '' AND CAF.COD_GL_GROUP IS NOT NULL THEN CAT.DES_GL_GROUP
     WHEN IFNULL(APR.COD_GL, '') = '' AND CAF_PR.COD_GL_GROUP IS NOT NULL THEN CAT_PR.DES_GL_GROUP
     WHEN APR.COD_GL != ACC.COD_GL_GROUP AND ACC.COD_GL_GROUP IS NOT NULL THEN CAT_ACC.DES_GL_GROUP
     ELSE CAT_APR.DES_GL_GROUP END AS DES_GL_GROUP,
CASE WHEN IFNULL(APR.COD_GL, '') = '' AND CAF.COD_GL_GROUP IS NOT NULL THEN CAT.ACCOUNT_CONCEPT
     WHEN IFNULL(APR.COD_GL, '') = '' AND CAF_PR.COD_GL_GROUP IS NOT NULL THEN CAT_PR.ACCOUNT_CONCEPT
     WHEN APR.COD_GL != ACC.COD_GL_GROUP AND ACC.COD_GL_GROUP IS NOT NULL THEN CAT_ACC.ACCOUNT_CONCEPT
     ELSE CAT_APR.ACCOUNT_CONCEPT END AS ACCOUNT_CONCEPT
FROM (
    SELECT TMP.* 
    FROM MIS_APR_${var:tabla} TMP 
    WHERE TMP.DATA_DATE='${var:periodo}') APR 
LEFT JOIN MIS_PAR_REL_CAF_OPER CAF
ON APR.COD_ENTITY = CAF.COD_ENTITY AND APR.COD_CURRENCY = CAF.COD_CURRENCY AND APR.COD_VALUE = CAF.COD_VALUE
    AND APR.COD_BLCE_STATUS = CAF.COD_BLCE_STATUS 
    AND APR.COD_PRODUCT = CAF.COD_PRODUCT AND APR.COD_SUBPRODUCT = CAF.COD_SUBPRODUCT
LEFT JOIN MIS_PAR_REL_CAF_OPER CAF_PR
ON APR.COD_ENTITY = CAF_PR.COD_ENTITY AND APR.COD_CURRENCY = CAF_PR.COD_CURRENCY AND APR.COD_VALUE = CAF_PR.COD_VALUE
    AND APR.COD_BLCE_STATUS = CAF_PR.COD_BLCE_STATUS 
    AND APR.COD_PRODUCT = CAF_PR.COD_PRODUCT AND TRIM(CAF_PR.COD_SUBPRODUCT)=''
LEFT JOIN (SELECT DISTINCT COD_ENTITY, COD_CURRENCY, COD_GL_GROUP FROM MIS_PAR_REL_CAF_ACC) ACC
ON ISNULL(CAF.COD_GL_GROUP,CAF_PR.COD_GL_GROUP) = ACC.COD_GL_GROUP 
AND ISNULL(CAF.COD_ENTITY,CAF_PR.COD_ENTITY) = ACC.COD_ENTITY 
AND ISNULL(CAF.COD_CURRENCY,CAF_PR.COD_CURRENCY) = ACC.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT
ON CAF.COD_GL_GROUP = CAT.COD_GL_GROUP AND CAF.COD_CURRENCY = CAT.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT_PR
ON CAF_PR.COD_GL_GROUP = CAT_PR.COD_GL_GROUP AND CAF_PR.COD_CURRENCY = CAT_PR.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT_ACC
ON ACC.COD_GL_GROUP = CAT_ACC.COD_GL_GROUP AND ACC.COD_CURRENCY = CAT_ACC.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT_APR
ON APR.COD_GL = CAT_APR.COD_GL_GROUP AND APR.COD_CURRENCY = CAT_APR.COD_CURRENCY
;

--COMPUTE INCREMENTAL STATS MIS_TMP_STG_${var:tabla}_1;

----Rechazo de registros sin agrupador contable
INSERT OVERWRITE MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'AGRUPADOR CONTABLE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', 'N/A', 'N/A', a.COD_PRODUCT, a.COD_SUBPRODUCT, 'N/A', a.COD_BLCE_STATUS, a.COD_VALUE, 
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_1 a 
WHERE a.COD_GL_GROUP IS NULL AND a.COD_CONT = 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_VALUE, a.COD_ENTITY, a.COD_CURRENCY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_BLCE_STATUS
;

INSERT OVERWRITE MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'AGRUPADOR CONTABLE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', 'N/A', 'N/A', a.COD_PRODUCT, a.COD_SUBPRODUCT, 'N/A', a.COD_BLCE_STATUS, a.COD_VALUE, 
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_1 a 
WHERE a.COD_GL_GROUP IS NULL AND a.COD_CONT <> 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_VALUE, a.COD_ENTITY, a.COD_CURRENCY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_BLCE_STATUS
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0; --Unicamente se guardan rechazos con saldos diferente de cero


------------ Creación de la tabla de Asignación de Cuenta P&G ------------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
CREATE TABLE MIS_TMP_STG_${var:tabla}_2 AS
SELECT SALIDA_CAF.*, 
    CASE WHEN REL_CG.IND_DETAIL = 'Y' THEN CONCAT(REL_CG.COD_PL_ACC,'/', COALESCE(REL_PG.COD_BLCE_PROD, REL_PG_PR.COD_BLCE_PROD, '')) 
         ELSE REL_CG.COD_PL_ACC END COD_PL_ACC, CAT.DES_PL_ACC
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_1 TMP 
    WHERE TMP.COD_GL_GROUP IS NOT NULL) SALIDA_CAF 
LEFT JOIN MIS_PAR_REL_PL_ACC REL_CG 
ON SALIDA_CAF.COD_ENTITY=REL_CG.COD_ENTITY AND SALIDA_CAF.COD_GL_GROUP=REL_CG.COD_GL_GROUP 
   AND SALIDA_CAF.COD_CURRENCY=REL_CG.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_PL CAT
ON REL_CG.COD_PL_ACC = CAT.COD_PL_ACC
LEFT JOIN MIS_PAR_REL_BP_OPER REL_PG
ON SALIDA_CAF.COD_ENTITY=REL_PG.COD_ENTITY AND SALIDA_CAF.COD_GL_GROUP=REL_PG.COD_GL_GROUP AND SALIDA_CAF.COD_CURRENCY=REL_PG.COD_CURRENCY
    AND SALIDA_CAF.COD_PRODUCT=REL_PG.COD_PRODUCT AND SALIDA_CAF.COD_SUBPRODUCT = REL_PG.COD_SUBPRODUCT
LEFT JOIN MIS_PAR_REL_BP_OPER REL_PG_PR
ON SALIDA_CAF.COD_ENTITY=REL_PG_PR.COD_ENTITY AND SALIDA_CAF.COD_GL_GROUP=REL_PG_PR.COD_GL_GROUP AND SALIDA_CAF.COD_CURRENCY=REL_PG_PR.COD_CURRENCY
    AND SALIDA_CAF.COD_PRODUCT=REL_PG_PR.COD_PRODUCT AND TRIM(REL_PG_PR.COD_SUBPRODUCT)=''
;

--COMPUTE INCREMENTAL STATS MIS_TMP_STG_${var:tabla}_2;

----Rechazo de registros sin cuenta P&G
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'CUENTA DE GESTION' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A',  
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_2 a 
WHERE a.COD_PL_ACC IS NULL AND a.COD_CONT = 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP
;

INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'CUENTA DE GESTION' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A',  
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_2 a 
WHERE a.COD_PL_ACC IS NULL AND a.COD_CONT <> 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;


----- Creación de la tabla de Asignación de Producto de Balance -----
--1. Asignar producto balance usando producto y subproducto (REL_PG)
--2. Asignar producto balance usando únicamente producto (REL_PG_PR)
---------------------------------------------------------------------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
CREATE TABLE MIS_TMP_STG_${var:tabla}_3 AS
SELECT SALIDA_CG.*, 
    CASE WHEN REL_PG.COD_BLCE_PROD IS NOT NULL THEN REL_PG.COD_BLCE_PROD ELSE REL_PG_PR.COD_BLCE_PROD END AS COD_BLCE_PROD, 
    CASE WHEN REL_PG.COD_BLCE_PROD IS NOT NULL THEN CAT.DES_BLCE_PROD ELSE CAT_PR.DES_BLCE_PROD END AS DES_BLCE_PROD 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_2 TMP 
    WHERE TMP.COD_PL_ACC IS NOT NULL) SALIDA_CG
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') b
ON SALIDA_CG.DATA_DATE = b.DATA_DATE AND SALIDA_CG.IDF_CTO = b.IDF_CTO AND SALIDA_CG.COD_ENTITY = b.COD_ENTITY 
LEFT JOIN MIS_PAR_REL_BP_OPER REL_PG
ON SALIDA_CG.COD_ENTITY=REL_PG.COD_ENTITY AND SALIDA_CG.COD_GL_GROUP=REL_PG.COD_GL_GROUP AND SALIDA_CG.COD_CURRENCY=REL_PG.COD_CURRENCY
    AND SALIDA_CG.COD_PRODUCT=REL_PG.COD_PRODUCT AND SALIDA_CG.COD_SUBPRODUCT = REL_PG.COD_SUBPRODUCT
    /*AND IF(SALIDA_CG.COD_ACT_TYPE IS NULL OR TRIM(SALIDA_CG.COD_ACT_TYPE) = '', 'NA', SALIDA_CG.COD_ACT_TYPE)=IF(REL_PG.COD_ACT_TYPE IS NULL OR TRIM(REL_PG.COD_ACT_TYPE) = '', 'NA', REL_PG.COD_ACT_TYPE) AND IF(b.COD_RATE_TYPE IS NULL OR TRIM(b.COD_RATE_TYPE) = '', 'NA', b.COD_RATE_TYPE) = IF(REL_PG.COD_RATE_TYPE IS NULL OR TRIM(REL_PG.COD_RATE_TYPE) = '', 'NA', REL_PG.COD_RATE_TYPE)*/
AND TRIM(REL_PG.COD_ACT_TYPE) = '' AND TRIM(REL_PG.COD_RATE_TYPE) = ''
LEFT JOIN MIS_PAR_REL_BP_OPER REL_PG_PR
ON SALIDA_CG.COD_ENTITY=REL_PG_PR.COD_ENTITY AND SALIDA_CG.COD_GL_GROUP=REL_PG_PR.COD_GL_GROUP AND SALIDA_CG.COD_CURRENCY=REL_PG_PR.COD_CURRENCY
    AND SALIDA_CG.COD_PRODUCT=REL_PG_PR.COD_PRODUCT AND TRIM(REL_PG_PR.COD_SUBPRODUCT)=''
/*AND (REL_PG_PR.COD_SUBPRODUCT=SALIDA_CG.COD_SUBPRODUCT OR TRIM(REL_PG_PR.COD_SUBPRODUCT)='')*/
    AND TRIM(REL_PG_PR.COD_ACT_TYPE) = '' AND TRIM(REL_PG_PR.COD_RATE_TYPE) = ''
LEFT JOIN MIS_PAR_CAT_BP CAT
ON REL_PG.COD_BLCE_PROD = CAT.COD_BLCE_PROD
LEFT JOIN MIS_PAR_CAT_BP CAT_PR
ON REL_PG_PR.COD_BLCE_PROD = CAT_PR.COD_BLCE_PROD;

--COMPUTE INCREMENTAL STATS MIS_TMP_STG_${var:tabla}_3;

----Rechazo de registros sin producto de balance
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'PRODUCTO DE BALANCE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, a.COD_ACT_TYPE, a.COD_PRODUCT, a.COD_SUBPRODUCT, 'N/A', 'N/A', 'N/A',   
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30, 10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30, 10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_3 a  
WHERE a.COD_BLCE_PROD IS NULL AND a.COD_CONT = 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE
;

INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL)  
PARTITION (DATA_DATE, COD_CONT)
SELECT 'PRODUCTO DE BALANCE' AS DIM_REJ, a.COD_ENTITY, a.COD_CURRENCY, 'N/A', a.COD_GL_GROUP, a.COD_ACT_TYPE, a.COD_PRODUCT, a.COD_SUBPRODUCT, 'N/A', 'N/A', 'N/A',   
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30, 10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30, 10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_3 a  
WHERE a.COD_BLCE_PROD IS NULL AND a.COD_CONT <> 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_ENTITY, a.COD_CURRENCY, a.COD_GL_GROUP, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;


--- Asignación de la dimensión línea de negocio ---
--1. Asignar LdN usando producto y tipo de cliente (RES_BL)
--2. Asignar LdN usando únicamente producto (RES_BL_PR)
---------------------------------------------------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
CREATE TABLE MIS_TMP_STG_${var:tabla}_4 AS
SELECT DWH.*, 
    CASE WHEN RES_BL.COD_BUSINESS_LINE IS NOT NULL THEN RES_BL.COD_BUSINESS_LINE ELSE RES_BL_PR.COD_BUSINESS_LINE END AS COD_BUSINESS_LINE, 
    CASE WHEN RES_BL.COD_BUSINESS_LINE IS NOT NULL THEN CAT.DES_BUSINESS_LINE ELSE CAT_PR.DES_BUSINESS_LINE END AS DES_BUSINESS_LINE,
    CONT.IDF_CLI, CLI.COD_TYP_CLNT 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_3 TMP 
    WHERE TMP.COD_BLCE_PROD IS NOT NULL) DWH 
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE='${var:periodo}') CONT
ON DWH.DATA_DATE = CONT.DATA_DATE AND DWH.IDF_CTO = CONT.IDF_CTO AND DWH.COD_ENTITY = CONT.COD_ENTITY 
LEFT JOIN (SELECT * FROM MIS_APR_CLIENT_DT WHERE DATA_DATE='${var:periodo}') CLI
ON CLI.DATA_DATE = CONT.DATA_DATE AND CLI.IDF_CLI = CONT.IDF_CLI
LEFT JOIN MIS_PAR_REL_BL_OPER RES_BL
ON DWH.COD_BLCE_PROD = RES_BL.COD_BLCE_PROD 
    AND CLI.COD_TYP_CLNT = RES_BL.COD_TYP_CLNT
    AND TRIM(RES_BL.COD_TYP_CLNT)!=''
LEFT JOIN MIS_PAR_REL_BL_OPER RES_BL_PR
ON DWH.COD_BLCE_PROD = RES_BL_PR.COD_BLCE_PROD 
    AND TRIM(RES_BL_PR.COD_TYP_CLNT)=''
LEFT JOIN MIS_PAR_CAT_BL CAT
ON RES_BL.COD_BUSINESS_LINE=CAT.COD_BUSINESS_LINE
LEFT JOIN MIS_PAR_CAT_BL CAT_PR
ON RES_BL_PR.COD_BUSINESS_LINE=CAT_PR.COD_BUSINESS_LINE;

--COMPUTE INCREMENTAL STATS MIS_TMP_STG_${var:tabla}_4;

--- Rechazo solo para contratos que no se encuentran en la mis_apr_client_dt
INSERT INTO MIS_REJECTIONS_DIMENSIONS_NULL
SELECT DATA_DATE, COD_CONT, COD_INFO_SOURCE, COD_ENTITY, COD_CURRENCY, IDF_CTO, 'N/A' AS COD_RATE_TYPE, IDF_CLI, COD_TYP_CLNT, EOPBAL_CAP, PL
FROM MIS_TMP_STG_${var:tabla}_4
WHERE COD_BUSINESS_LINE IS NULL AND COD_TYP_CLNT IS NULL AND (ISNULL(EOPBAL_CAP, 0) <> 0 OR ISNULL(PL, 0) <> 0)
;

----Rechazo de registros sin línea de negocio     
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'LINEA DE NEGOCIO' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    a.COD_BLCE_PROD, a.IDF_CLI, a.COD_TYP_CLNT, a.IDF_CTO, 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NULL AND a.COD_CONT = 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_BLCE_PROD, a.IDF_CLI, a.COD_TYP_CLNT, a.IDF_CTO
;

INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, TYP_DOC_ID, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'LINEA DE NEGOCIO' AS DIM_REJ, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    a.COD_BLCE_PROD, a.IDF_CLI, a.COD_TYP_CLNT, a.IDF_CTO, 'N/A', CAST(SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS AMO_PL, a.DATA_DATE, a.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 a 
WHERE a.COD_BUSINESS_LINE IS NULL AND a.COD_CONT <> 'CTA'
GROUP BY a.DATA_DATE, a.COD_CONT, a.COD_BLCE_PROD, a.IDF_CLI, a.COD_TYP_CLNT, a.IDF_CTO
HAVING SUM(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0)) <> 0 OR SUM(a.PL) <> 0;


----Inserción de registros en la tabla de dimensiones DWH
ALTER TABLE MIS_DWH_${var:tabla}
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla} PARTITION (DATA_DATE, COD_INFO_SOURCE)
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, 
    a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, 
    a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, 
    NULL, NULL, NULL, NULL, NULL, NULL, a.DATA_DATE, a.COD_INFO_SOURCE 
FROM MIS_TMP_STG_${var:tabla}_4 a
WHERE a.COD_BUSINESS_LINE IS NOT NULL;

--- Crear partición en APR si no existe ---
ALTER TABLE MIS_DWH_${var:tabla} ADD IF NOT EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='NA');
--COMPUTE INCREMENTAL STATS MIS_DWH_${var:tabla} PARTITION (DATA_DATE = '${var:periodo}');


--- Eliminación de tablas temporales ---
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;