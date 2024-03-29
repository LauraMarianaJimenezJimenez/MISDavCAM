------------------------------------------------ MOTOR DE CONCILIACIÓN ------------------------------------------------ 

--- Selección de la Base de Datos Correcta ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Creación Tabla de Conciliación Auxiliar ---
TRUNCATE TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL;
DROP TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL;
CREATE TABLE MIS_TMP_DWH_OPERACIONAL (
DATA_DATE STRING, COD_CONT STRING, COD_ACCO_CENT STRING, COD_CURRENCY STRING, COD_ENTITY STRING, COD_GL_GROUP STRING, DES_GL_GROUP STRING, 
ACCOUNT_CONCEPT STRING, EOPBAL_CAP DECIMAL(20,2), EOPBAL_INT DECIMAL(20,2), AVGBAL_CAP DECIMAL(20,2), PL DECIMAL(20,2));

INSERT INTO MIS_TMP_DWH_OPERACIONAL
SELECT TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS DECIMAL(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS DECIMAL(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS DECIMAL(20, 2)) AS PL 
FROM MIS_DWH_ASSETS TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
UNION ALL
SELECT TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP,
TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS DECIMAL(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS DECIMAL(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS DECIMAL(20, 2)) AS PL 
FROM MIS_DWH_LIABILITIES TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
UNION ALL
SELECT TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP,
TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS DECIMAL(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS DECIMAL(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS DECIMAL(20, 2)) AS PL 
FROM MIS_DWH_PROVISIONS TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
UNION ALL
SELECT TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP,
TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS DECIMAL(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS DECIMAL(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS DECIMAL(20, 2)) AS PL 
FROM MIS_DWH_OFF_BALANCE TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
UNION ALL
SELECT TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP,
TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS DECIMAL(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS DECIMAL(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS DECIMAL(20, 2)) AS PL 
FROM MIS_DWH_FEES TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
UNION ALL
SELECT TMP.DATA_DATE, TMP.COD_CONT, NULL AS COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT, 
    CAST(SUM(TMP.EOPBAL_CAP) AS decimal(20, 2)) AS EOPBAL_CAP, CAST(SUM(TMP.EOPBAL_INT) AS decimal(20, 2)) AS EOPBAL_INT, CAST(SUM(TMP.AVGBAL_CAP) AS decimal(20, 2)) AS AVGBAL_CAP, CAST(SUM(TMP.PL) AS decimal(20, 2)) AS PL 
FROM MIS_DWH_EXPENSES TMP
WHERE TMP.DATA_DATE='${var:periodo}'
GROUP BY TMP.DATA_DATE, TMP.COD_CONT, TMP.COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP, TMP.ACCOUNT_CONCEPT
;

--- Información operacional en Moneda Local  ---
TRUNCATE TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL_RPT;
DROP TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL_RPT;
CREATE TABLE MIS_TMP_DWH_OPERACIONAL_RPT AS 
SELECT DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ACCO_CENT, 'USD' AS COD_CURRENCY, 
    DWH.COD_ENTITY, DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, 
    CASE
        WHEN DWH.COD_CURRENCY='USD' THEN DWH.EOPBAL_CAP
        ELSE DWH.EOPBAL_CAP * RT.EXCH_RATE 
        END AS EOPBAL_CAP, 
    CASE
        WHEN DWH.COD_CURRENCY='USD' THEN DWH.EOPBAL_INT 
        ELSE DWH.EOPBAL_INT * RT.EXCH_RATE 
        END AS EOPBAL_INT, 
    CASE 
        WHEN DWH.COD_CURRENCY='USD' THEN DWH.PL 
        ELSE DWH.PL * RT.EXCH_RATE 
        END AS PL 
FROM MIS_TMP_DWH_OPERACIONAL DWH 
LEFT JOIN MIS_PAR_EXCH_RATE RT
ON DWH.DATA_DATE = RT.DATA_DATE AND RT.COD_CONT = 'TC_FOTO' AND DWH.COD_CURRENCY = RT.COD_CURRENCY
AND IF(ISNULL(TRIM(RT.COD_ENTITY),'') = '' ,'1',DWH.COD_ENTITY) = IF(ISNULL(TRIM(RT.COD_ENTITY),'') = '','1',RT.COD_ENTITY)
;

--- Creación Tabla de Conciliación Auxiliar ---
TRUNCATE TABLE IF EXISTS MIS_DWH_RECONCILIATION_AUX;
DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_AUX;
CREATE TABLE MIS_DWH_RECONCILIATION_AUX (
DATA_DATE STRING, COD_CONT STRING, COD_ACCO_CENT STRING,
COD_CURRENCY STRING,
COD_ENTITY STRING, EOPBAL_CAP DECIMAL(20,2),
EOPBAL_INT DECIMAL(20,2), AVGBAL_CAP DECIMAL(20,2),
AVGBAL_INT DECIMAL(20,2), PL DECIMAL(20,2),
COD_INFO_SOURCE STRING, COD_GL_GROUP STRING,
DES_GL_GROUP STRING, ACCOUNT_CONCEPT STRING);


--- Generación de conciliación a partir de las tablas apropiadas, verificando si la información es netamente contable, operacional o proviene de ambas fuentes ------------
INSERT INTO MIS_DWH_RECONCILIATION_AUX
SELECT 
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.DATA_DATE
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.DATA_DATE 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.DATA_DATE END AS DATA_DATE,
--- Asigna el código apropiado según la fuente de la información ---
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN 'RCCL'
         WHEN CONT.DATA_DATE IS NOT NULL THEN 'CTBP' --Si agrupador está en parametría operacional, RCTB; sino, CTBP.
         WHEN OPER.DATA_DATE IS NOT NULL THEN 'RCCL' END AS COD_CONT, 
--- Selecciona las variables del flujo contable u operacional, según corresponda ---         
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ACCO_CENT
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ACCO_CENT 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_ACCO_CENT END AS COD_ACCO_CENT,
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_CURRENCY
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_CURRENCY 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_CURRENCY END AS COD_CURRENCY,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ENTITY
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ENTITY 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_ENTITY END AS COD_ENTITY,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_CAP - OPER.TOT_EOPBAL_CAP) AS decimal(15, 2)) 
         WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_CAP) AS decimal(15, 2))
         WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_EOPBAL_CAP) AS decimal(15, 2)) END AS EOPBAL_CAP,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_INT - OPER.TOT_EOPBAL_INT) AS decimal(15, 2)) 
         WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_INT) AS decimal(15, 2))
         WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_EOPBAL_INT) AS decimal(15, 2)) END AS EOPBAL_INT,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN 0 
         WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_CAP) AS decimal(15, 2))
         WHEN OPER.DATA_DATE IS NOT NULL THEN 0 /*CAST((-1 * OPER.TOT_AVGBAL_CAP) AS decimal(15, 2))*/ END AS AVGBAL_CAP, 
    0 AS AVGBAL_INT,
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_PL - OPER.TOT_PL) AS decimal(15, 2)) 
         WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_PL) AS decimal(15, 2))
         WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_PL) AS decimal(15, 2)) END AS PL, 
    'RECL' AS COD_INFO_SOURCE,
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_GL_GROUP
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_GL_GROUP 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_GL_GROUP END AS COD_GL_GROUP,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.DES_GL_GROUP
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.DES_GL_GROUP 
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.DES_GL_GROUP END AS DES_GL_GROUP,  
    CASE WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.ACCOUNT_CONCEPT
         WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.ACCOUNT_CONCEPT
         WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.ACCOUNT_CONCEPT END AS ACCOUNT_CONCEPT
FROM (
    SELECT TMP.DATA_DATE, '0' AS COD_ACCO_CENT, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP,
        TMP.ACCOUNT_CONCEPT, ISNULL(SUM(TMP.EOPBAL_CAP),0) AS TOT_EOPBAL_CAP,/* ISNULL(SUM(TMP.AVGBAL_CAP),0) AS TOT_AVGBAL_CAP,*/ ISNULL(SUM(TMP.EOPBAL_INT),0) AS TOT_EOPBAL_INT, ISNULL(SUM(TMP.PL),0) AS TOT_PL 
    FROM MIS_TMP_DWH_OPERACIONAL_RPT TMP
    WHERE TMP.DATA_DATE='${var:periodo}'
    GROUP BY TMP.DATA_DATE, TMP.COD_CURRENCY, TMP.COD_ENTITY, TMP.COD_GL_GROUP, TMP.DES_GL_GROUP,
        TMP.ACCOUNT_CONCEPT) OPER 
FULL JOIN (
    SELECT ACC.DATA_DATE, ACC.COD_ACCO_CENT, ACC.COD_CURRENCY, ACC.COD_ENTITY, ACC.COD_GL_GROUP, ACC.DES_GL_GROUP, 
        ACC.ACCOUNT_CONCEPT,
        ISNULL(SUM(ACC.EOPBAL_CAP),0) AS TOT_EOPBAL_CAP, /*ISNULL(SUM(ACC.AVGBAL_CAP),0) AS TOT_AVGBAL_CAP,*/ ISNULL(SUM(ACC.EOPBAL_INT),0) AS TOT_EOPBAL_INT, ISNULL(SUM(ACC.PL),0) AS TOT_PL
    FROM MIS_DWH_ACCOUNTING ACC
    WHERE ACC.DATA_DATE='${var:periodo}'
    GROUP BY ACC.DATA_DATE, ACC.COD_ACCO_CENT, ACC.COD_CURRENCY, ACC.COD_ENTITY, ACC.COD_GL_GROUP, ACC.DES_GL_GROUP,
        ACC.ACCOUNT_CONCEPT) CONT 
ON OPER.DATA_DATE=CONT.DATA_DATE AND OPER.COD_CURRENCY=CONT.COD_CURRENCY AND 
   OPER.COD_ENTITY=CONT.COD_ENTITY AND OPER.COD_GL_GROUP=CONT.COD_GL_GROUP  ----OMIDITO COD_ACCO_CENT
;

ALTER TABLE MIS_REJECTIONS_DIMENSIONS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_CONT = 'RCCL');

ALTER TABLE MIS_REJECTIONS_DIMENSIONS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_CONT = 'RCTB');

ALTER TABLE MIS_REJECTIONS_DIMENSIONS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_CONT = 'CTBP');

------------ Creación de la tabla de Asignación de Cuenta P&G ------------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_1;
DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_1;
CREATE TABLE MIS_TMP_STG_RECONC_1 AS
SELECT SALIDA_CAF.*, REL_CG.COD_PL_ACC, CAT.DES_PL_ACC 
FROM (
    SELECT TMP.* 
    FROM MIS_DWH_RECONCILIATION_AUX TMP 
    WHERE (TMP.EOPBAL_CAP <> 0 OR TMP.EOPBAL_INT <> 0 OR TMP.AVGBAL_CAP <> 0 OR TMP.AVGBAL_INT <> 0 OR TMP.PL <> 0)) SALIDA_CAF 
LEFT JOIN MIS_PAR_REL_PL_ACC REL_CG 
ON SALIDA_CAF.COD_ENTITY=REL_CG.COD_ENTITY AND SALIDA_CAF.COD_GL_GROUP=REL_CG.COD_GL_GROUP AND SALIDA_CAF.COD_CURRENCY=REL_CG.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_PL CAT
ON REL_CG.COD_PL_ACC = CAT.COD_PL_ACC;

------------ Inserción Tabla de Rechazos de Cuenta P&G ------------
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'CUENTA DE GESTION' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, 'N/A', DWH.COD_GL_GROUP,   
    CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_RECONC_1 DWH 
WHERE DWH.COD_PL_ACC IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;

------------ Creación de la tabla de Asignación de Producto de Balance ------------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_2;
DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_2;
CREATE TABLE MIS_TMP_STG_RECONC_2 AS
SELECT SALIDA_CG.*, REL_PG.COD_BLCE_PROD, CAT.DES_BLCE_PROD 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_RECONC_1 TMP 
    WHERE TMP.COD_PL_ACC IS NOT NULL) SALIDA_CG 
LEFT JOIN MIS_PAR_REL_BP_ACC REL_PG
ON SALIDA_CG.COD_ENTITY=REL_PG.COD_ENTITY AND SALIDA_CG.COD_GL_GROUP=REL_PG.COD_GL_GROUP AND SALIDA_CG.COD_CURRENCY=REL_PG.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_BP CAT
ON REL_PG.COD_BLCE_PROD = CAT.COD_BLCE_PROD;

------------ Inserción Tabla de Rechazos de Producto de Balance ------------
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'PRODUCTO DE BALANCE' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, 'N/A', DWH.COD_GL_GROUP, 
    CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_RECONC_2 DWH 
WHERE DWH.COD_BLCE_PROD IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;

--- Asignación de la dimensión línea de negocio ---
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_3;
DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_3;
CREATE TABLE MIS_TMP_STG_RECONC_3 AS
SELECT DWH.*, RES_BL.COD_BUSINESS_LINE, CAT.DES_BUSINESS_LINE 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_RECONC_2 TMP 
    WHERE TMP.COD_BLCE_PROD IS NOT NULL) DWH 
LEFT JOIN MIS_PAR_REL_BL_ACC RES_BL
ON DWH.COD_ENTITY = RES_BL.COD_ENTITY AND DWH.COD_GL_GROUP = RES_BL.COD_GL_GROUP
LEFT JOIN MIS_PAR_CAT_BL CAT
ON RES_BL.COD_BUSINESS_LINE=CAT.COD_BUSINESS_LINE;

--- Rechazo de registros sin línea de negocio --- 
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'LINEA DE NEGOCIO' AS DIM_REJ, DWH.COD_ENTITY, 'N/A', 'N/A', DWH.COD_GL_GROUP,
    CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_RECONC_3 DWH 
WHERE DWH.COD_BUSINESS_LINE IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;

--- Asignación de dimensión tipo y familia de gasto ---
-- AL1. Asignación por agrupador, centro y código
-- AL2. Asignación por centro y código
-- AL3. Asignación por agrupador y código
-- AL4. Asignación por agrupador y centro
-- AL5. Asignación por código
-- AL6. Asignación por centro
-- AL7. Asignación por agrupador
-- AL1, AL2, AL3 y AL5 no aplican en conciliación, campo código gasto es operacional
DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_4;
CREATE TABLE MIS_TMP_STG_RECONC_4 AS
SELECT DWH.*, 
    CASE 
        WHEN AL4.EXP_TYPE IS NOT NULL THEN AL4.EXP_TYPE
        WHEN AL6.EXP_TYPE IS NOT NULL THEN AL6.EXP_TYPE
        WHEN AL7.EXP_TYPE IS NOT NULL THEN AL7.EXP_TYPE
    END AS EXP_TYPE,
    CASE 
        WHEN AL4.EXP_FAMILY IS NOT NULL THEN AL4.EXP_FAMILY
        WHEN AL6.EXP_FAMILY IS NOT NULL THEN AL6.EXP_FAMILY
        WHEN AL7.EXP_FAMILY IS NOT NULL THEN AL7.EXP_FAMILY
    END AS EXP_FAMILY
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_RECONC_3 TMP 
    WHERE TMP.COD_BUSINESS_LINE IS NOT NULL
) DWH   
LEFT JOIN MIS_PAR_REL_EXP_TYP AL4 
ON TRIM(AL4.COD_EXPENSE) = '' AND DWH.COD_ACCO_CENT=AL4.COD_ACCO_CENT AND DWH.COD_GL_GROUP=AL4.COD_GL_GROUP 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL6 
ON COALESCE(AL4.EXP_FAMILY) IS NULL 
   AND TRIM(AL6.COD_EXPENSE) = '' AND DWH.COD_ACCO_CENT=AL6.COD_ACCO_CENT AND TRIM(AL6.COD_GL_GROUP) = '' 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL7 
ON COALESCE(AL4.EXP_FAMILY, AL6.EXP_FAMILY) IS NULL 
   AND TRIM(AL7.COD_EXPENSE) = '' AND TRIM(AL7.COD_ACCO_CENT) = '' AND DWH.COD_GL_GROUP=AL7.COD_GL_GROUP 
;

--- Limpieza de partición ----
ALTER TABLE MIS_DWH_RECONCILIATION
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Inserción en tabla definitiva de conciliación ---
INSERT INTO MIS_DWH_RECONCILIATION PARTITION (DATA_DATE, COD_CONT)
SELECT DWH.COD_ACCO_CENT, '0' AS COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY,
    DWH.EOPBAL_CAP, DWH.EOPBAL_INT, DWH.AVGBAL_CAP, DWH.AVGBAL_INT, DWH.PL, 'RECL' AS COD_INFO_SOURCE, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC,
    DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, 
    NULL, NULL, NULL, NULL, NULL, 
    DWH.EXP_TYPE, DWH.EXP_FAMILY, 
    DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_RECONC_4 DWH 
WHERE DWH.COD_BUSINESS_LINE IS NOT NULL;

DROP TABLE IF EXISTS MIS_TMP_LOAD_AVGBAL_CONC;
CREATE TABLE MIS_TMP_LOAD_AVGBAL_CONC
(COD_GL_GROUP STRING, COD_BLCE_STATUS STRING,
COD_ENTITY STRING, COD_CURRENCY STRING, COD_ACCO_CENT STRING, COD_INFO_SOURCE STRING, COD_BLCE_PROD STRING, DES_BLCE_PROD STRING,
COD_PL_ACC STRING, DES_PL_ACC STRING, COD_BUSINESS_LINE STRING, DES_BUSINESS_LINE STRING,
AVGBAL_CAP DECIMAL(30,10), AVGBAL_INT DECIMAL(30,10));

INSERT INTO MIS_TMP_LOAD_AVGBAL_CONC
    SELECT a.COD_GL_GROUP, COD_BLCE_STATUS, a.COD_ENTITY, COD_CURRENCY, COD_ACCO_CENT, 'A' AS COD_INFO_SOURCE, COD_BLCE_PROD, DES_BLCE_PROD,
COD_PL_ACC, DES_PL_ACC, COD_BUSINESS_LINE, DES_BUSINESS_LINE, CAST(SUM(b.AVGBAL_CAP) AS DECIMAL(30,10)), CAST(SUM(b.AVGBAL_INT) AS DECIMAL(30,10))
FROM MIS_DWH_RECONCILIATION a
LEFT JOIN (SELECT COD_GL_GROUP, COD_ENTITY, CAST((SUM(EOPBAL_CAP)/(DATEDIFF(to_timestamp('${var:periodo}','yyyyMMdd'),TRUNC(to_timestamp('${var:periodo}','yyyyMMdd'),'MM'))+1)) AS DECIMAL(30,10))AS AVGBAL_CAP,
 CAST((SUM(EOPBAL_INT)/(DATEDIFF(to_timestamp('${var:periodo}','yyyyMMdd'),TRUNC(to_timestamp('${var:periodo}','yyyyMMdd'),'MM'))+1)) AS DECIMAL(30,10))AS AVGBAL_INT
    FROM MIS_DWH_RECONCILIATION
    WHERE Year(TO_TIMESTAMP(DATA_DATE,'yyyyMMdd')) =  Year(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'))
    AND Month(TO_TIMESTAMP(DATA_DATE,'yyyyMMdd')) = Month(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'))
    AND DATA_DATE <= '${var:periodo}'
    AND COD_PL_ACC = 'NO_PYG'
    AND COD_CONT IN ('RCCL','RCBA','RCIN','RCLI','CTBP','RCRP')
    GROUP BY COD_GL_GROUP, COD_ENTITY) b
ON a.COD_GL_GROUP = b.COD_GL_GROUP AND a.COD_ENTITY = b.COD_ENTITY
WHERE DATA_DATE =  '${var:periodo}'
    AND COD_PL_ACC = 'NO_PYG'
    AND COD_CONT IN ('RCCL','RCBA','RCIN','RCLI','CTBP','RCRP')
    GROUP BY a.COD_GL_GROUP, COD_BLCE_STATUS, a.COD_ENTITY, COD_CURRENCY, COD_ACCO_CENT, COD_BLCE_PROD, DES_BLCE_PROD,
COD_PL_ACC, DES_PL_ACC, COD_BUSINESS_LINE, DES_BUSINESS_LINE;

DROP TABLE IF EXISTS MIS_TMP_DWH_RECONCILIATION;
CREATE TABLE MIS_TMP_DWH_RECONCILIATION AS
SELECT * FROM MIS_DWH_RECONCILIATION
WHERE DATA_DATE = '${var:periodo}';

ALTER TABLE MIS_DWH_RECONCILIATION
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_RECONCILIATION PARTITION (DATA_DATE, COD_CONT)
SELECT DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY,
    DWH.EOPBAL_CAP, DWH.EOPBAL_INT, CASE WHEN DWH.COD_ENTITY = '04' THEN DWH.EOPBAL_CAP ELSE b.AVGBAL_CAP END AS AVGBAL_CAP, 
    CASE WHEN DWH.COD_ENTITY = '04' THEN DWH.EOPBAL_INT ELSE b.AVGBAL_INT END AS AVGBAL_INT, DWH.PL, DWH.COD_INFO_SOURCE, 
    ISNULL(DWH.COD_GL_GROUP,b.COD_GL_GROUP) AS COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, 
    ISNULL(DWH.COD_PL_ACC,b.COD_PL_ACC) AS COD_PL_ACC, ISNULL(DWH.DES_PL_ACC,b.DES_PL_ACC) AS DES_PL_ACC,
    ISNULL(DWH.COD_BLCE_PROD,b.COD_BLCE_PROD) AS COD_BLCE_PROD, ISNULL(DWH.DES_BLCE_PROD,b.DES_BLCE_PROD) AS DES_BLCE_PROD, 
    ISNULL(DWH.COD_BUSINESS_LINE,b.COD_BUSINESS_LINE) AS COD_BUSINESS_LINE, ISNULL(DWH.DES_BUSINESS_LINE,b.DES_BUSINESS_LINE) AS DES_BUSINESS_LINE, 
    DWH.FTP, DWH.FTP_RESULT, DWH.IND_POOL, DWH.COD_SEGMENT, DWH.DES_SEGMENT, 
    DWH.EXP_TYPE, DWH.EXP_FAMILY, 
    DWH.DATA_DATE, DWH.COD_CONT
FROM MIS_TMP_DWH_RECONCILIATION DWH
LEFT JOIN MIS_TMP_LOAD_AVGBAL_CONC b
on DWH.COD_GL_GROUP = b.COD_GL_GROUP AND DWH.COD_BLCE_STATUS = b.COD_BLCE_STATUS AND DWH.COD_ENTITY = b.COD_ENTITY AND DWH.COD_CURRENCY = b.COD_CURRENCY AND ISNULL(DWH.COD_ACCO_CENT,'0') = ISNULL(b.COD_ACCO_CENT,'0')
WHERE DWH.DATA_DATE = '${var:periodo}';

--- Eliminación de tablas temporales ---
--TRUNCATE TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL;
--DROP TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL;
--TRUNCATE TABLE IF EXISTS MIS_DWH_RECONCILIATION_AUX;
--DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_AUX;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_1;
--DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_1;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_2;
--DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_2;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_3;
--DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_3;
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_RECONC_4;
--DROP TABLE IF EXISTS MIS_TMP_STG_RECONC_4;