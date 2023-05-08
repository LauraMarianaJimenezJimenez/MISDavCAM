--------------------------------------------------------------- ASIG. DIM. CONTABLE -------------------------------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

------------ Asignación del Agrupador Contable ------------
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
CREATE TABLE MIS_TMP_STG_${var:tabla}_1 AS
SELECT APR.*, CAF.COD_GL_GROUP, CAT.DES_GL_GROUP, CAT.ACCOUNT_CONCEPT 
FROM (
    SELECT TMP.* 
    FROM MIS_APR_${var:tabla} TMP 
    WHERE TMP.DATA_DATE='${var:periodo}') APR 
LEFT JOIN MIS_PAR_REL_CAF_ACC CAF
ON APR.COD_GL = CAF.COD_GL AND APR.COD_ENTITY = CAF.COD_ENTITY AND APR.COD_CURRENCY = CAF.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT
ON CAF.COD_GL_GROUP = CAT.COD_GL_GROUP AND CAF.COD_CURRENCY=CAT.COD_CURRENCY
;

------------ Insercion Tabla de Rechazos de Agrupador Contable ------------
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'AGRUPADOR CONTABLE' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_1 DWH 
WHERE DWH.COD_GL_GROUP IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0; --Inicamente se guardan rechazos con saldos diferente de cero


------------ Creación de la tabla de Asignación de Cuenta P&G ------------
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
CREATE TABLE MIS_TMP_STG_${var:tabla}_2 AS
SELECT SALIDA_CAF.*, REL_CG.COD_PL_ACC, CAT.DES_PL_ACC 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_1 TMP 
    WHERE TMP.COD_GL_GROUP IS NOT NULL) SALIDA_CAF 
LEFT JOIN MIS_PAR_REL_PL_ACC REL_CG 
ON SALIDA_CAF.COD_ENTITY=REL_CG.COD_ENTITY AND SALIDA_CAF.COD_GL_GROUP=REL_CG.COD_GL_GROUP AND SALIDA_CAF.COD_CURRENCY=REL_CG.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_PL_ACC CAT
ON REL_CG.COD_PL_ACC = CAT.COD_PL_ACC;

------------ Inserción Tabla de Rechazos de Cuenta P&G ------------
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'CUENTA DE GESTION' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, 'N/A', DWH.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_2 DWH 
WHERE DWH.COD_PL_ACC IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;


------------ Creación de la tabla de Asignación de Producto de Balance ------------
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
CREATE TABLE MIS_TMP_STG_${var:tabla}_3 AS
SELECT SALIDA_CG.*, REL_PG.COD_BLCE_PROD, CAT.DES_BLCE_PROD 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_2 TMP 
    WHERE TMP.COD_PL_ACC IS NOT NULL) SALIDA_CG 
LEFT JOIN MIS_PAR_REL_BP_ACC REL_PG
ON SALIDA_CG.COD_ENTITY=REL_PG.COD_ENTITY AND SALIDA_CG.COD_GL_GROUP=REL_PG.COD_GL_GROUP AND SALIDA_CG.COD_CURRENCY=REL_PG.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_BLCE_PROD CAT
ON REL_PG.COD_BLCE_PROD = CAT.COD_BLCE_PROD;

------------ Inserción Tabla de Rechazos de Producto de Balance ------------
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'PRODUCTO DE BALANCE' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, 'N/A', DWH.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_3 DWH 
WHERE DWH.COD_BLCE_PROD IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;


--- Asignación de la dimensión línea de negocio ---
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
CREATE TABLE MIS_TMP_STG_${var:tabla}_4 AS
SELECT DWH.*, RES_BL.COD_BUSINESS_LINE, CAT.DES_BUSINESS_LINE 
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_3 TMP 
    WHERE TMP.COD_BLCE_PROD IS NOT NULL) DWH 
LEFT JOIN MIS_PAR_REL_BL_ACC RES_BL
ON DWH.COD_ENTITY = RES_BL.COD_ENTITY AND DWH.COD_GL_GROUP = RES_BL.COD_GL_GROUP
LEFT JOIN MIS_PAR_CAT_BL CAT
ON RES_BL.COD_BUSINESS_LINE=CAT.COD_BUSINESS_LINE;

--- Rechazo de registros sin línea de negocio --- 
INSERT INTO MIS_REJECTIONS_DIMENSIONS (DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, AMO_EOPBAL, AMO_PL) 
PARTITION (DATA_DATE, COD_CONT)
SELECT 'LINEA DE NEGOCIO' AS DIM_REJ, DWH.COD_ENTITY, 'N/A', 'N/A', DWH.COD_GL_GROUP, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_4 DWH 
WHERE DWH.COD_BUSINESS_LINE IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_GL_GROUP
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0;


--- Asignación de la dimensión tipo y familia de gasto ---
-- AL1. Asignación por agrupador, centro y código
-- AL2. Asignación por centro y código
-- AL3. Asignación por agrupador y código
-- AL4. Asignación por agrupador y centro
-- AL5. Asignación por código
-- AL6. Asignación por centro
-- AL7. Asignación por agrupador
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_5;
CREATE TABLE MIS_TMP_STG_${var:tabla}_5 AS
SELECT DWH.*, 
    CASE 
        WHEN AL1.EXP_TYPE IS NOT NULL THEN AL1.EXP_TYPE
        WHEN AL2.EXP_TYPE IS NOT NULL THEN AL2.EXP_TYPE
        WHEN AL3.EXP_TYPE IS NOT NULL THEN AL3.EXP_TYPE
        WHEN AL4.EXP_TYPE IS NOT NULL THEN AL4.EXP_TYPE
        WHEN AL5.EXP_TYPE IS NOT NULL THEN AL5.EXP_TYPE
        WHEN AL6.EXP_TYPE IS NOT NULL THEN AL6.EXP_TYPE
        WHEN AL7.EXP_TYPE IS NOT NULL THEN AL7.EXP_TYPE
    END AS EXP_TYPE,
    CASE 
        WHEN AL1.EXP_FAMILY IS NOT NULL THEN AL1.EXP_FAMILY
        WHEN AL2.EXP_FAMILY IS NOT NULL THEN AL2.EXP_FAMILY
        WHEN AL3.EXP_FAMILY IS NOT NULL THEN AL3.EXP_FAMILY
        WHEN AL4.EXP_FAMILY IS NOT NULL THEN AL4.EXP_FAMILY
        WHEN AL5.EXP_FAMILY IS NOT NULL THEN AL5.EXP_FAMILY
        WHEN AL6.EXP_FAMILY IS NOT NULL THEN AL6.EXP_FAMILY
        WHEN AL7.EXP_FAMILY IS NOT NULL THEN AL7.EXP_FAMILY
    END AS EXP_FAMILY
FROM (
    SELECT TMP.* 
    FROM MIS_TMP_STG_${var:tabla}_4 TMP 
    WHERE TMP.COD_BUSINESS_LINE IS NOT NULL
) DWH 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL1 
ON DWH.COD_EXPENSE=AL1.COD_EXPENSE AND DWH.COD_ACCO_CENT=AL1.COD_ACCO_CENT AND DWH.COD_GL_GROUP=AL1.COD_GL_GROUP 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL2 
ON COALESCE(AL1.EXP_FAMILY) IS NULL 
   AND DWH.COD_EXPENSE=AL2.COD_EXPENSE AND DWH.COD_ACCO_CENT=AL2.COD_ACCO_CENT AND TRIM(AL2.COD_GL_GROUP) = '' 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL3 
ON COALESCE(AL1.EXP_FAMILY, AL2.EXP_FAMILY) IS NULL 
   AND DWH.COD_EXPENSE=AL3.COD_EXPENSE AND TRIM(AL3.COD_ACCO_CENT) = '' AND DWH.COD_GL_GROUP=AL3.COD_GL_GROUP    
LEFT JOIN MIS_PAR_REL_EXP_TYP AL4 
ON COALESCE(AL1.EXP_FAMILY, AL2.EXP_FAMILY, AL3.EXP_FAMILY) IS NULL 
   AND TRIM(AL4.COD_EXPENSE) = '' AND DWH.COD_ACCO_CENT=AL4.COD_ACCO_CENT AND DWH.COD_GL_GROUP=AL4.COD_GL_GROUP 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL5 
ON COALESCE(AL1.EXP_FAMILY, AL2.EXP_FAMILY, AL3.EXP_FAMILY, AL4.EXP_FAMILY) IS NULL
   AND DWH.COD_EXPENSE=AL5.COD_EXPENSE AND TRIM(AL5.COD_ACCO_CENT) = '' AND TRIM(AL5.COD_GL_GROUP) = '' 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL6 
ON COALESCE(AL1.EXP_FAMILY, AL2.EXP_FAMILY, AL3.EXP_FAMILY, AL4.EXP_FAMILY, AL5.EXP_FAMILY) IS NULL 
   AND TRIM(AL6.COD_EXPENSE) = '' AND DWH.COD_ACCO_CENT=AL6.COD_ACCO_CENT AND TRIM(AL6.COD_GL_GROUP) = '' 
LEFT JOIN MIS_PAR_REL_EXP_TYP AL7 
ON COALESCE(AL1.EXP_FAMILY, AL2.EXP_FAMILY, AL3.EXP_FAMILY, AL4.EXP_FAMILY, AL5.EXP_FAMILY, AL6.EXP_FAMILY) IS NULL 
   AND TRIM(AL7.COD_EXPENSE) = '' AND TRIM(AL7.COD_ACCO_CENT) = '' AND DWH.COD_GL_GROUP=AL7.COD_GL_GROUP 
;

--- Limpieza de partición ----
ALTER TABLE MIS_DWH_${var:tabla}
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

------------ Insercion de registros en tabla DWH ------------
INSERT INTO MIS_DWH_${var:tabla} PARTITION (DATA_DATE, COD_CONT)
SELECT DWH.COD_GL, DWH.DES_GL, DWH.COD_ACCO_CENT, DWH.COD_EXPENSE, DWH.COD_OFFI, DWH.COD_NAR, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, 
    DWH.COD_ENTITY, DWH.EOPBAL_CAP, DWH.EOPBAL_INT, DWH.AVGBAL_CAP, DWH.AVGBAL_INT, DWH.PL, DWH.COD_INFO_SOURCE, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC,
    DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, 
    NULL, NULL, NULL, NULL, NULL, 
    DWH.EXP_TYPE, DWH.EXP_FAMILY, 
    DWH.DATA_DATE, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_5 DWH 
WHERE DWH.COD_BUSINESS_LINE IS NOT NULL;


------ Eliminación de tablas temporales ------
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_2;
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_3;
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_4;
TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_5;
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_5;