--------------------------------------------------------------- ASIG. DIM. CONTABLE ---------------------------------------------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

COMPUTE INCREMENTAL STATS MIS_APR_${var:tabla} partition (data_date = '${var:periodo}');

------------ Creacion de la tabla Stage 1 de Contabilidad ------------
DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
CREATE TABLE MIS_TMP_STG_${var:tabla}_1 AS
SELECT APR.*,CAF.COD_GL_GROUP, CAT.DES_GL_GROUP, CAT.ACCOUNT_CONCEPT 
FROM (
    SELECT APR_AUX.* 
    FROM MIS_APR_${var:tabla} APR_AUX 
    WHERE APR_AUX.COD_GL IS NOT NULL AND APR_AUX.DATA_DATE='${var:periodo}') APR 
LEFT JOIN MIS_PAR_REL_CAF_ACC CAF 
ON APR.COD_GL = CAF.COD_GL AND APR.COD_ENTITY = CAF.COD_ENTITY AND APR.COD_CURRENCY = CAF.COD_CURRENCY
LEFT JOIN MIS_PAR_CAT_CAF CAT 
ON CAF.COD_GL_GROUP=CAT.COD_GL_GROUP AND CAF.COD_ENTITY = CAT.COD_ENTITY AND CAF.COD_CURRENCY=CAT.COD_CURRENCY;

------------ Insercion Tabla de Rechazos de Agrupador Contable ------------
INSERT OVERWRITE MIS_REJECTIONS_DIMENSIONS (DATA_DATE, DIM_REJ, COD_ENTITY, COD_CURRENCY, COD_GL, COD_GL_GROUP, COD_ACT_TYPE, COD_PRODUCT, COD_SUBPRODUCT, COD_RATE_TYPE, COD_BLCE_STATUS, COD_VALUE, 
    COD_BLCE_PROD, COD_MANAGER, COD_TYP_CLNT, COD_SEGMENT, COD_COUNTRY, AMO_EOPBAL, AMO_PL) 
PARTITION (COD_CONT)
SELECT DWH.DATA_DATE, 'AGRUPADOR CONTABLE' AS DIM_REJ, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', CAST(SUM(DWH.EOPBAL_CAP) AS DECIMAL(30,10)) AS AMO_EOPBAL, CAST(SUM(DWH.PL) AS DECIMAL(30,10)) AS AMO_PL, DWH.COD_CONT 
FROM MIS_TMP_STG_${var:tabla}_1 DWH 
WHERE DWH.COD_GL_GROUP IS NULL
GROUP BY DWH.DATA_DATE, DWH.COD_CONT, DWH.COD_ENTITY, DWH.COD_CURRENCY, DWH.COD_GL
HAVING SUM(DWH.EOPBAL_CAP) <> 0 OR SUM(DWH.PL) <> 0; --Inicamente se guardan rechazos con saldos diferente de cero


------------ Insercion en la tabla definitiva ------------
ALTER TABLE MIS_DWH_${var:tabla}
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla} PARTITION (DATA_DATE)
SELECT DWH.COD_CONT, DWH.COD_GL, DWH.DES_GL, DWH.COD_ACCO_CENT, DWH.COD_OFFI, DWH.COD_NAR, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, DWH.EOPBAL_CAP, DWH.EOPBAL_INT, 
    DWH.AVGBAL_CAP, DWH.AVGBAL_INT, DWH.PL, 
    CASE WHEN DWH.COD_CURRENCY="COP" THEN CAST(DWH.EOPBAL_CAP / RT.EXCH_RATE AS decimal(30, 10))
        ELSE CAST(DWH.EOPBAL_CAP * RT.EXCH_RATE AS decimal(30, 10))
        END AS EOPBAL_CAP_RPT, 
    CASE WHEN DWH.COD_CURRENCY="COP" THEN CAST(DWH.PL / RT.EXCH_RATE AS decimal(30, 10))
        ELSE CAST(DWH.PL * RT.EXCH_RATE AS decimal(30, 10)) 
        END AS PL_RPT, 
    DWH.COD_INFO_SOURCE, DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.DATA_DATE
FROM (
    SELECT TMP.* FROM MIS_TMP_STG_${var:tabla}_1 TMP
    WHERE TMP.COD_GL_GROUP IS NOT NULL) DWH
LEFT JOIN MIS_PAR_EXCH_RATE RT
ON DWH.DATA_DATE = RT.DATA_DATE AND DWH.COD_ENTITY = RT.COD_ENTITY AND DWH.COD_CURRENCY = RT.COD_CURRENCY;


------------ Eliminar Tablas Temporales ------------
--TRUNCATE TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;
--DROP TABLE IF EXISTS MIS_TMP_STG_${var:tabla}_1;