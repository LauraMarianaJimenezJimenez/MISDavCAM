--------------------------------------------------------- MIS_APR_OFF_BALANCE --------------------------------------------------------
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load

TRUNCATE TABLE IF EXISTS mis_load_trade; 
LOAD DATA INPATH '${var:ruta_fuentes_comisiones}/Comisiones_Trade.csv' INTO TABLE mis_load_trade;

TRUNCATE TABLE IF EXISTS mis_load_subastas; 
LOAD DATA INPATH '${var:ruta_fuentes_comisiones}/Subastas_Consolidadas.csv' INTO TABLE mis_load_subastas;

ALTER TABLE MIS_APR_FEES_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Aprovisionamiento de Comisiones de subasta
INSERT INTO MIS_APR_FEES_M
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'COM' AS COD_CONT, '9999999999' AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, cod_acco_cent, NULL AS COD_OFFI, 'VIG' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, 'HNL' AS COD_CURRENCY,
'1' AS COD_ENTITY, strleft(tip_convocatoria,3) AS COD_PRODUCT, 'COM' AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
CAST(comision AS DECIMAL(30,10)) AS PL, 'SUBASTA' AS COD_INFO_SOURCE, 'VAR_PERIODO' AS DATA_DATE
FROM MIS_LOAD_SUBASTAS
WHERE tip_convocatoria NOT IN ('BIB')
;

----Aprovisionamiento de Comisiones de trade
INSERT INTO MIS_APR_FEES_M
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'COM' AS COD_CONT, '9999999999' AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, 'VIG' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, 
'HNL' AS COD_CURRENCY, '1' AS COD_ENTITY, TRIM(strleft(producto,4)) AS COD_PRODUCT, 'COM' AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(comision AS DECIMAL(30,10)) AS PL, 'TRADE' AS COD_INFO_SOURCE, 'VAR_PERIODO' AS DATA_DATE
FROM MIS_LOAD_TRADE;
;

ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='CDIF03');

COMPUTE INCREMENTAL STATS MIS_APR_CONTRACT_DT partition (data_date = '${var:periodo}');

----Aprovisionamiento de Contratos asociados a Contingentes
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='CDIF03') 
SELECT a.C3NUMP AS IDF_CTO, '1' AS COD_ENTITY, CAST(a.C3PROD AS STRING) AS COD_PRODUCT, 'COM' AS COD_SUBPRODUCT, IFNULL(NULL, '') AS COD_ACT_TYPE, CASE WHEN CAST(c3mone AS STRING) IN ('1','2') THEN 'USD' ELSE 'HNL' END AS COD_CURRENCY, ISNULL(CUX1CS,'99999999') AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, 
NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, NULL AS RATE_INT,
NULL AS DATE_ORIGIN, NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
NULL AS EXP_DATE, NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, 
NULL AS COD_UNI_FRE_REV_INT, NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, NULL AS INI_AM, NULL AS CUO_AM, 
NULL AS CREDIT_LIM_AM, NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC,
NULL AS COD_SELLER, NULL AS DES_SELLER
FROM MIS_LOAD_CDIF03 a
LEFT JOIN (select * from mis_load_cup00901 where cast(cux1ap as string) in ('50','51') and CUXREL in ('SOW', 'JAF', 'JOF', 'OWN')) b
on C3NUMP  =  CUX1AC
WHERE C3NUMP NOT IN (SELECT DISTINCT IDF_CTO FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}')
;