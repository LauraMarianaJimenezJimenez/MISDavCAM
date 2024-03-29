--------------------------------------------------------------- MIS_APR_FEES ---------------------------------------------------------------
USE ${var:base_datos}; 
SET DECIMAL_V2=FALSE;

--Limpieza de partición del día en ejecución
ALTER TABLE MIS_APR_FEES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Llenado de Tablas de Carga
TRUNCATE TABLE IF EXISTS MIS_LOAD_CSD533GLF; 
LOAD DATA INPATH '${var:ruta_fuentes_comisiones}/CSD533GLF.CSV' INTO TABLE MIS_LOAD_CSD533GLF;

----Aprovisionamiento de Comisiones ganadas y Gastos comisiones (Menos Tarjetas de Crédito)
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
--Se agrupan los saldos para consolidar las comisiones por contrato del dia de ejecucion, y acumularlas con las comisiones del día anterior
SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, b.COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(SUM(a.PL) AS decimal(30, 10)) AS PL, a.COD_INFO_SOURCE
FROM (
    --Comisiones registradas en el dia de ejecucion
    SELECT 'COM' AS COD_CONT, a.TRAACC AS IDF_CTO, a.TRAGLN AS COD_GL, NULL AS DES_GL, a.TRACCN AS COD_ACCO_CENT, 
        NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
        CASE WHEN STRLEFT(a.TRAGLN,1) = '7' THEN 'RSL'
             WHEN STRLEFT(a.TRAGLN,1) = '6' THEN 'RSL'
             ELSE 'N/A' END AS COD_VALUE,
        a.TRACCY AS COD_CURRENCY, a.TRABNK AS COD_ENTITY, 
        CASE WHEN b.COD_PRODUCT IS NOT NULL THEN b.COD_PRODUCT
             WHEN c.COD_PRODUCT IS NOT NULL THEN c.COD_PRODUCT 
             WHEN d.COD_PRODUCT IS NOT NULL THEN d.COD_PRODUCT ELSE 'COM_DEF' END AS COD_PRODUCT, 
        CASE WHEN b.COD_SUBPRODUCT IS NOT NULL THEN b.COD_SUBPRODUCT
             WHEN c.COD_SUBPRODUCT IS NOT NULL THEN c.COD_SUBPRODUCT 
             WHEN d.COD_SUBPRODUCT IS NOT NULL THEN d.COD_SUBPRODUCT ELSE 'COM_DEF' END AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
         CASE WHEN TRIM(a.TRADCC) = '5' THEN a.TRAAMT*-1 ELSE a.TRAAMT END AS PL, 'TRANS' AS COD_INFO_SOURCE
    FROM MIS_LOAD_TRANS a
    LEFT JOIN MIS_APR_ASSETS b 
    ON b.DATA_DATE = '${var:periodo}' AND a.TRAACC = b.IDF_CTO AND a.TRACCY = b.COD_CURRENCY AND a.TRABNK = b.COD_ENTITY AND b.COD_VALUE = 'CAP'
    LEFT JOIN MIS_APR_LIABILITIES c 
    ON c.DATA_DATE = '${var:periodo}' AND a.TRAACC = c.IDF_CTO AND a.TRACCY = c.COD_CURRENCY AND a.TRABNK = c.COD_ENTITY AND c.COD_VALUE = 'CAP'
    LEFT JOIN MIS_APR_OFF_BALANCE d 
    ON d.DATA_DATE = '${var:periodo}' AND a.TRAACC = d.IDF_CTO AND a.TRACCY = d.COD_CURRENCY AND a.TRABNK = d.COD_ENTITY AND d.COD_VALUE = 'CAP'
    WHERE (a.TRAGLN IN (SELECT COD_GL FROM MIS_PAR_COM_GL WHERE COD_TABLE = 'TRANS'))
    AND a.TRABTH NOT IN ('6951')
    AND (b.COD_PRODUCT IS NOT NULL OR c.COD_PRODUCT IS NOT NULL OR d.COD_PRODUCT IS NOT NULL)
    UNION ALL
    --Comisiones registradas en el dia anterior
    SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
        a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE,
        a.PL, a.COD_INFO_SOURCE
    FROM MIS_APR_FEES a
    WHERE DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) > 1 AND 
        TO_TIMESTAMP(a.DATA_DATE, 'yyyyMMdd') = DAYS_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1) 
    AND COD_INFO_SOURCE = 'TRANS' 
) AS A
LEFT JOIN  (SELECT IDF_CTO, COD_ACT_TYPE FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}' ) b
ON A.IDF_CTO = b.IDF_CTO
GROUP BY a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, b.COD_ACT_TYPE, a.COD_INFO_SOURCE;  

----Aprovisionamiento de Comisiones ganadas y Gastos comisiones (Tarjetas de Crédito)
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
--Se agrupan los saldos para consolidar las comisiones por contrato del dia de ejecucion, y acumularlas con las comisiones del día anterior
SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, b.COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(SUM(a.PL) AS decimal(30, 10)) AS PL, a.COD_INFO_SOURCE
FROM (
    --Comisiones registradas en el dia de ejecucion
    SELECT 'COM_TAR' AS COD_CONT, CAST(CAST(CSD_TAR AS BIGINT) AS STRING) AS IDF_CTO, COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS 
    COD_BLCE_STATUS, CASE WHEN STRLEFT(COD_GL,1) = '7' THEN 'COM_GSTO'
    WHEN STRLEFT(COD_GL,1) = '6' THEN 'COM_GNDA' ELSE 'N/A' END AS COD_VALUE, 'USD' AS COD_CURRENCY, 
    '01' AS COD_ENTITY, SUBSTR(CSD_TAR,4,1) AS COD_PRODUCT, SUBSTR(CSD_TAR,4,8) AS COD_SUBPRODUCT, '' AS 
    COD_ACT_TYPE, CAST(SUM(PL)*-1 AS DECIMAL(30,10)) AS PL, 'CSD533GLF' AS COD_INFO_SOURCE 
    FROM
    (SELECT CSD_TAR, 
    CASE 
    WHEN STRLEFT(CSD_ACCT,1) IN ('6','7') 
    THEN CSD_ACCT 
    ELSE CSD_ACCTCP END AS COD_GL, 
    CASE WHEN STRLEFT(CSD_ACCT,1) IN ('6','7') THEN SUM(CSD_AMOUNT)*-1 WHEN STRLEFT(CSD_ACCTCP,1) IN 
    ('6','7') THEN SUM(CSD_AMOUNT) END AS PL
    FROM MIS_LOAD_CSD533GLF
    WHERE CSD_DATE = '${var:periodo}'
    AND (STRLEFT(CSD_ACCT,1) IN ('6','7') OR STRLEFT(CSD_ACCTCP,1) IN ('6','7'))
    GROUP BY CSD_TAR, CSD_ACCT, CSD_ACCTCP) A
    WHERE (a.COD_GL IN (SELECT COD_GL FROM MIS_PAR_COM_GL WHERE COD_TABLE = 'CSD'))
    GROUP BY CSD_TAR, COD_GL
    UNION ALL
    --Comisiones registradas en el dia anterior
    SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
        a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE,
        a.PL, a.COD_INFO_SOURCE
    FROM MIS_APR_FEES a
    WHERE DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) > 1 
    AND TO_TIMESTAMP(a.DATA_DATE, 'yyyyMMdd') = DAYS_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1) 
    AND COD_INFO_SOURCE = 'CSD533GLF'
) AS A
LEFT JOIN (SELECT IDF_CTO, COD_ACT_TYPE FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}' ) b
ON A.IDF_CTO = b.IDF_CTO
GROUP BY a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, b.COD_ACT_TYPE,
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, a.COD_INFO_SOURCE;