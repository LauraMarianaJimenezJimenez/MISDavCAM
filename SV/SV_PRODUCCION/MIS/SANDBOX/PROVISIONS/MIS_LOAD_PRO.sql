--------------------------------------------------------------- MIS_APR_PROVISIONS -------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_WFDLRES; 
LOAD DATA INPATH '${var:ruta_fuentes_provisiones}/WFDLRES.CSV' INTO TABLE MIS_LOAD_WFDLRES;

ALTER TABLE MIS_APR_PROVISIONS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Aprovisionamiento de Provisiones Capital ---
INSERT INTO MIS_APR_PROVISIONS 
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'PROV' AS COD_CONT, CAST(CAST(a.RESACC AS BIGINT) AS string) AS IDF_CTO, CAST(a.RESGLR AS string) AS COD_GL, NULL AS DES_GL,
NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, 
'1' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, 'USD' AS COD_CURRENCY,
 '01' AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL 
THEN b.COD_PRODUCT ELSE c.COD_PRODUCT END AS COD_PRODUCT, 
c.COD_SUBPRODUCT AS COD_SUBPRODUCT, 
c.COD_ACT_TYPE AS COD_ACT_TYPE,
CAST(-1*(a.RESMRC+a.RESMRI) AS DECIMAL(30,10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
NULL AS PL, 'WFDLRES' AS COD_INFO_SOURCE 
FROM MIS_LOAD_WFDLRES a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(CAST(a.RESACC AS BIGINT) AS string) = B.IDF_CTO
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') c
ON CAST(CAST(a.RESACC AS BIGINT) AS STRING) = STRRIGHT(c.IDF_CTO,16) 
WHERE CAST(CAST(a.RESACC AS BIGINT) AS STRING) NOT IN ('707068045')
;

--- Aprovisionamiento de Provisiones P&G ---
INSERT INTO MIS_APR_PROVISIONS 
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'PROV' AS COD_CONT, CAST(CAST(a.RESACC AS BIGINT) AS string) AS IDF_CTO, CAST(d.COD_GL_PL AS string) AS COD_GL, NULL AS DES_GL,
NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, 
'0' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, 'USD' AS COD_CURRENCY,
 '01' AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL 
THEN b.COD_PRODUCT ELSE e.COD_PRODUCT END AS COD_PRODUCT, 
e.COD_SUBPRODUCT AS COD_SUBPRODUCT,
e.COD_ACT_TYPE AS COD_ACT_TYPE,
NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
CAST(((a.RESMRC+a.RESMRI+(CASE WHEN c.IDF_CTO IS NULL THEN 0 ELSE c.EOPBAL_CAP END))) AS DECIMAL(30,10)) AS PL, 'WFDLRES' AS COD_INFO_SOURCE 
FROM MIS_LOAD_WFDLRES a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(CAST(a.RESACC AS BIGINT) AS string) = B.IDF_CTO
LEFT JOIN (SELECT * FROM MIS_APR_PROVISIONS WHERE COD_VALUE = 'CAP' AND COD_INFO_SOURCE NOT LIKE '%_M' ) c
ON TO_TIMESTAMP(c.DATA_DATE, 'yyyyMMdd') = DATE_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 'MM'),1) AND CAST(CAST(a.RESACC AS BIGINT) AS STRING) = c.IDF_CTO
LEFT JOIN MIS_PAR_REL_PROV_GL d
ON TRIM(a.RESGLR) = TRIM(d.COD_GL_CAP)
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') e
ON CAST(CAST(a.RESACC AS BIGINT) AS STRING) = STRRIGHT(e.IDF_CTO,16)
WHERE d.COD_GL_PL IS NOT NULL
;

--Aprovisionamiento de contratos que no cruzan en el día y se pone el balance del cierre del mes anterior--
INSERT INTO MIS_APR_PROVISIONS 
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'PROV' AS COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, e.COD_ACCO_CENT, e.COD_OFFI, a.COD_BLCE_STATUS, 
'RSL' AS COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, e.COD_PRODUCT,
e.COD_SUBPRODUCT, e.COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
CAST(ISNULL(c.EOPBAL_CAP,0) AS DECIMAL(30,10)) AS PL, 'WFDLRES' AS COD_INFO_SOURCE
FROM (SELECT DISTINCT IDF_CTO, COD_GL, DES_GL, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY 
FROM MIS_APR_PROVISIONS
WHERE TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1) AND COD_VALUE = 'RSL') a
LEFT JOIN (SELECT * FROM MIS_APR_PROVISIONS WHERE DATA_DATE = '${var:periodo}' AND COD_VALUE = 'RSL') b 
ON a.IDF_CTO = b.IDF_CTO
LEFT JOIN (SELECT IDF_CTO, SUM(EOPBAL_CAP) AS EOPBAL_CAP FROM MIS_APR_PROVISIONS WHERE TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') = DATE_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 'MM'),1) AND COD_VALUE = 'CAP' AND COD_INFO_SOURCE NOT LIKE '%_M'
GROUP BY IDF_CTO) c
ON a.IDF_CTO = c.IDF_CTO
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') e
ON a.IDF_CTO = e.IDF_CTO
WHERE b.IDF_CTO IS NULL AND e.IDF_CTO IS NOT NULL
;