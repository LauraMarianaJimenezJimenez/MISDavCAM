------------------------------------------------------------- MIS_APR_LIABILITIES ---------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_AHBKR001CD; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/AHBKR001CD.CSV' INTO TABLE MIS_LOAD_AHBKR001CD;
TRUNCATE TABLE IF EXISTS MIS_LOAD_AHBKR001CM; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/AHBKR001CM.CSV' INTO TABLE MIS_LOAD_AHBKR001CM;
TRUNCATE TABLE IF EXISTS MIS_LOAD_CCBKR001; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CCBKR001.CSV' INTO TABLE MIS_LOAD_CCBKR001;
TRUNCATE TABLE IF EXISTS MIS_LOAD_MBALANCE; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/MBALANCE.CSV' INTO TABLE MIS_LOAD_MBALANCE;
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUVENORE; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CUVENORE.CSV' INTO TABLE MIS_LOAD_CUVENORE;
TRUNCATE TABLE IF EXISTS MIS_LOAD_CCRETENCION; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CCRETENCION.CSV' INTO TABLE MIS_LOAD_CCRETENCION;
TRUNCATE TABLE IF EXISTS MIS_LOAD_AHRETENCION; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/AHRETENCION.CSV' INTO TABLE MIS_LOAD_AHRETENCION;
TRUNCATE TABLE IF EXISTS MIS_LOAD_EST_PORT_100; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ESTADO_PORTAFOLIO_100.CSV' INTO TABLE MIS_LOAD_EST_PORT_100;
TRUNCATE TABLE IF EXISTS MIS_LOAD_EST_PORT_87; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ESTADO_PORTAFOLIO_87.CSV' INTO TABLE MIS_LOAD_EST_PORT_87;
TRUNCATE TABLE IF EXISTS MIS_LOAD_EST_PORT_88; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ESTADO_PORTAFOLIO_88.CSV' INTO TABLE MIS_LOAD_EST_PORT_88;


--- Limpieza de partición ----
ALTER TABLE MIS_APR_LIABILITIES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Aprovisionamiento de cuentas ahorro - débito
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.CTA_BCO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.SALDO_CONTABLE AS decimal(30, 10)) AS EOPBAL_CAP,
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL,
    'AHBKR001CD' AS COD_INFO_SOURCE
FROM MIS_LOAD_AHBKR001CD a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO)
WHERE a.SALDO_CONTABLE >= 0;

----Aprovisionamiento de cuentas maestras
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.CTA_BCO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.SALDO_CONTABLE AS decimal(30, 10)) AS EOPBAL_CAP,
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL,
    'AHBKR001CM' AS COD_INFO_SOURCE
FROM MIS_LOAD_AHBKR001CM a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO)
WHERE a.SALDO_CONTABLE >= 0;

----Aprovisionamiento de cuentas corrientes
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.CTA_BCO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD'
        WHEN '6' THEN 'EUR' END AS COD_CURRENCY,
    '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CASE 
        WHEN STRLEFT(TRIM(a.CTA_BCO), 2) = '97' THEN CAST(a.SALDO_CONTABLE AS decimal(30, 10))
        ELSE CAST(-1 * a.SALDO_CONTABLE AS decimal(30, 10))
    END AS EOPBAL_CAP, 
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL,
    'CCBKR001' AS COD_INFO_SOURCE
FROM MIS_LOAD_CCBKR001 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO)
WHERE a.SALDO_CONTABLE >= 0;


--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT 
DROP IF EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='AHBKR001CD');

----Aprovisionamiento de contratos de cuentas ahorro - débito
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='AHBKR001CD') 
SELECT TRIM(a.CTA_BCO) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY, 
    TRIM(a.COD_CLIENTE) AS IDF_CLI, NULL AS COD_ACCO_CENT, 
    TRIM(a.OFIC_GESTION) AS COD_OFFI, '360' AS COD_BCA_INT, 
    NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, NULL AS RATE_INT, 
    CONCAT(SPLIT_PART(a.FECHA_APER, '/', 3), SPLIT_PART(a.FECHA_APER, '/', 2), SPLIT_PART(a.FECHA_APER, '/', 1)) AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, NULL AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, 'M' AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, NULL AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, TRIM(a.CANAL_DE_APERTURA) AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV,
    TRIM(a.COD_OFICIAL) AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.FECHA_APER, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB,
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_AHBKR001CD a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO);

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT 
DROP IF EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='AHBKR001CM');

----Aprovisionamiento de contratos de cuentas maestras
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='AHBKR001CM') 
SELECT TRIM(a.CTA_BCO) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY, 
    TRIM(a.COD_CLIENTE) AS IDF_CLI, NULL AS COD_ACCO_CENT, 
    TRIM(a.OFIC_GESTION) AS COD_OFFI, '360' AS COD_BCA_INT, 
    NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, NULL AS RATE_INT, 
    CONCAT(SPLIT_PART(a.FECHA_APER, '/', 3), SPLIT_PART(a.FECHA_APER, '/', 2), SPLIT_PART(a.FECHA_APER, '/', 1)) AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, NULL AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, 'M' AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, NULL AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, TRIM(a.CANAL_DE_APERTURA) AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV, 
    TRIM(a.COD_OFICIAL) AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.FECHA_APER, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB,
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_AHBKR001CM a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO);

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT 
DROP IF EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='CCBKR001');

----Aprovisionamiento de contratos de cuentas maestras
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='CCBKR001') 
SELECT TRIM(a.CTA_BCO) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.PROD_BANC)) AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD'
        WHEN '6' THEN 'EUR' END AS COD_CURRENCY,
    TRIM(a.COD_CLIENTE) AS IDF_CLI, NULL AS COD_ACCO_CENT, 
    TRIM(a.OFIC_GESTION) AS COD_OFFI, '360' AS COD_BCA_INT, 
    NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, NULL AS RATE_INT, 
    CONCAT(SPLIT_PART(a.FECHA_APER, '/', 3), SPLIT_PART(a.FECHA_APER, '/', 2), SPLIT_PART(a.FECHA_APER, '/', 1)) AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, NULL AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, 'M' AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, NULL AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV,
    TRIM(a.COD_OFICIAL) AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.FECHA_APER, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB, 
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_CCBKR001 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CTA_BCO);


----Aprovisionamiento de pasivos plazo - Capital
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, TRIM(a.NRO_DEPOSITO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    CASE TRIM(a.ESTADO) 
        WHEN 'RENOVADO' THEN '5'
        WHEN 'CANCELADO' THEN '2' 
        ELSE '1' END AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), CONCAT(TRIM(a.TIPO_CDI), IF(TRIM(a.PIG) = 'S', '_G', ''))) AS COD_PRODUCT, 
    '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CAST(-1* a.SALDO_ACTUAL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 
    'MBALANCE' AS COD_INFO_SOURCE 
FROM MIS_LOAD_MBALANCE a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.NRO_DEPOSITO)
WHERE TRIM(a.ESTADO) != 'PAGADO'; 

----Aprovisionamiento de pasivos plazo - Interés
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, TRIM(a.NRO_DEPOSITO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    CASE TRIM(a.ESTADO) 
        WHEN 'RENOVADO' THEN '5'
        WHEN 'CANCELADO' THEN '2' 
        ELSE '1' END AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), CONCAT(TRIM(a.TIPO_CDI), IF(TRIM(a.PIG) = 'S', '_G', ''))) AS COD_PRODUCT, 
    '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1* a.SALDO_INTERES AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 
    'MBALANCE' AS COD_INFO_SOURCE
FROM MIS_LOAD_MBALANCE a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.NRO_DEPOSITO)
WHERE TRIM(a.ESTADO) NOT IN ('PAGADO', 'CANCELADO'); 

/*
----Aprovisionamiento de pasivos plazo - PL
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, TRIM(a.NRO_DEPOSITO) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    CASE TRIM(a.ESTADO) 
        WHEN 'RENOVADO' THEN '5'
        WHEN 'CANCELADO' THEN '2' 
        ELSE '1' END AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), CONCAT(TRIM(a.TIPO_CDI), IF(TRIM(a.PIG) = 'S', '_G', ''))) AS COD_PRODUCT, 
    '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(a.provision as DECIMAL(30,10)) AS PL, 
    'MBALANCE' AS COD_INFO_SOURCE 
FROM MIS_LOAD_MBALANCE a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.NRO_DEPOSITO)
WHERE TRIM(a.ESTADO) != 'PAGADO'; 
*/


--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT 
DROP IF EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='MBALANCE');

----Aprovisionamiento de contratos de pasivo plazo
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='MBALANCE') 
SELECT TRIM(a.NRO_DEPOSITO) AS IDF_CTO, TRIM('4') AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), CONCAT(TRIM(a.TIPO_CDI), IF(TRIM(a.PIG) = 'S', '_G', ''))) AS COD_PRODUCT, 
    '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY, 
    TRIM(CAST(CAST(a.COD_CLIENTE AS BIGINT) AS STRING)) AS IDF_CLI, NULL AS COD_ACCO_CENT, a.SUC_GEST AS COD_OFFI, 
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, CAST(a.TASA/100 AS decimal(30, 10)) AS RATE_INT, 
    CONCAT(SPLIT_PART(a.FECHAVALOR, '/', 3), SPLIT_PART(a.FECHAVALOR, '/', 2), SPLIT_PART(a.FECHAVALOR, '/', 1)) AS DATE_ORIGIN, 
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
    CONCAT(SPLIT_PART(a.FECHAVEN, '/', 3), SPLIT_PART(a.FECHAVEN, '/', 2), SPLIT_PART(a.FECHAVEN, '/', 1)) AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, TRIM(a.PCUPON) AS COD_UNI_FREQ_INT_PAY, 
    NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, 
    a.MONTO_APERTURA AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, 
    TRIM(a.OFICIAL) AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV,
    TRIM(a.OFICIAL) AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.FECHA_CREACION, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB, 
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_MBALANCE a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.NRO_DEPOSITO); 


----Aprovisionamiento de cupones vencidos no redimidos
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, TRIM(a.CDI) AS IDF_CTO,
    NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY,
    '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), 'CUVENORE') AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * SUM(a.VALOR_NETO) AS decimal(30, 10)) AS EOPBAL_CAP,
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL,
    'CUVENORE' AS COD_INFO_SOURCE
FROM MIS_LOAD_CUVENORE a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CDI)
WHERE TRIM(a.PIGNORADO) != 'PAGADO'
GROUP BY TRIM(a.CDI), TRIM(a.MONEDA), COALESCE(TRIM(SPE.COD_PRODUCT), 'CUVENORE');

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT 
DROP IF EXISTS PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='CUVENORE');

----Aprovisionamiento de contratos de cupones vencidos no redimidos
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='CUVENORE') 
SELECT TRIM(a.CDI) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), 'CUVENORE') AS COD_PRODUCT, '' AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA) 
        WHEN '20' THEN 'CRC' 
        WHEN '0' THEN 'USD' END AS COD_CURRENCY, 
    MAX(TRIM(a.ENTE)) AS IDF_CLI, NULL AS COD_ACCO_CENT, 
    NULL AS COD_OFFI, NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 
    'F' AS COD_RATE_TYPE, NULL AS RATE_INT, 
    NULL AS DATE_ORIGIN, NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
    MAX(CONCAT(SPLIT_PART(a.FECHA_VENC_CUP, '/', 3), SPLIT_PART(a.FECHA_VENC_CUP, '/', 2), SPLIT_PART(a.FECHA_VENC_CUP, '/', 1))) AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, 
    NULL AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV, 
    MAX(TRIM(a.EJECUTIVO)) AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    NULL AS DATE_DISB, 
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_CUVENORE a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.CDI)
WHERE TRIM(a.CDI) NOT IN (SELECT DISTINCT TRIM(b.NRO_DEPOSITO) NRO_DEPOSITO FROM MIS_LOAD_MBALANCE b)
GROUP BY TRIM(a.CDI), TRIM(a.MONEDA), COALESCE(TRIM(SPE.COD_PRODUCT), 'CUVENORE');


------------------- Estado de portafolio 100 -------------------

----Aprovisionamiento de Prestamos con corresponsales
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.COD_2) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.SALDO_LIBROS AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_100' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_100 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Interes Prestamos con corresponsales
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.COD_2) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.INT_ACUMUL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_100' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_100 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
WHERE TRIM(a.COD_2) != '39'
;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_100');

----Aprovisionamiento de Contrato Prestamos con corresponsales
INSERT INTO MIS_APR_CONTRACT_DT
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_100') 
SELECT 
    TRIM(a.INVERSION) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.COD_2) AS COD_SUBPRODUCT, 
    NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY,
    TRIM(a.EMISOR) AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, NULL AS COD_RATE_TYPE,
    CAST(a.TASA/100 AS DECIMAL(30, 10)) AS RATE_INT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA,  'd/M/yyyy'), 'yyyyMMdd') AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.VENCIMIENTO, 'd/M/yyyy'), 'yyyyMMdd') AS EXP_DATE,
    1 AS FREQ_INT_PAY, 'D' AS COD_UNI_FREQ_INT_PAY, 
    NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT,
    CAST(DATEDIFF(TO_TIMESTAMP(a.COMPRA, 'd/M/yyyy'),TO_TIMESTAMP(a.VENCIMIENTO, 'd/M/yyyy')) AS smallint) AS AMRT_TRM, 
    'D' AS COD_UNI_AMRT_TRM,   
    a.COSTO AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV, 
    NULL AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA, 'd/M/yyyy'), 'yyyyMMdd') AS DATE_DISB,
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_EST_PORT_100 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

------------------- Estado de portafolio 87 -------------------

---Aprovisionamiento de Capital Captaciones de Banco Central
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.COSTO AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_87' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_87 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

---Aprovisionamiento de Prima Captaciones de Banco Central
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.ACU_DES_PRI AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_87' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_87 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_87');

----Aprovisionamiento de Contrato Captaciones de Banco Central
INSERT INTO MIS_APR_CONTRACT_DT
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_87') 
SELECT 
    TRIM(a.INVERSION) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, 
    NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY,
    TRIM(a.EMISOR) AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, NULL AS COD_RATE_TYPE,
    CAST(a.TASA/100 AS DECIMAL(30, 10)) AS RATE_INT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA,  'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.VENCIMIENTO, 'dd/MM/yyyy'), 'yyyyMMdd') AS EXP_DATE,
    1 AS FREQ_INT_PAY, 'D' AS COD_UNI_FREQ_INT_PAY, 
    NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT,
    CAST(DATEDIFF(TO_TIMESTAMP(a.COMPRA, 'dd/MM/yyyy'),TO_TIMESTAMP(a.VENCIMIENTO, 'dd/MM/yyyy')) AS smallint) AS AMRT_TRM, 
    'D' AS COD_UNI_AMRT_TRM,   
    a.COSTO AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV, 
    NULL AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB, 
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_EST_PORT_87 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

------------------- Estado de portafolio 88 -------------------

---Aprovisionamiento de Capital Captaciones de Banco Central
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD_INSTRUMENTO)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.COSTO AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_88' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_88 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

---Aprovisionamiento de Prima Captaciones de Banco Central
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD_INSTRUMENTO)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.ACU_DES_PRI AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_88' AS COD_INFO_SOURCE
FROM MIS_LOAD_EST_PORT_88 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_88');

----Aprovisionamiento de Contrato Captaciones de Banco Central
INSERT INTO MIS_APR_CONTRACT_DT
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_88') 
SELECT 
    TRIM(a.INVERSION) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD_INSTRUMENTO)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, 
    NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY,
    TRIM(a.EMISOR) AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, NULL AS COD_RATE_TYPE,
    CAST(a.TASA/100 AS DECIMAL(30, 10)) AS RATE_INT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA,  'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_ORIGIN,
    NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.VENCIMIENTO, 'dd/MM/yyyy'), 'yyyyMMdd') AS EXP_DATE,
    1 AS FREQ_INT_PAY, 'D' AS COD_UNI_FREQ_INT_PAY, 
    NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT,
    CAST(DATEDIFF(TO_TIMESTAMP(a.COMPRA, 'dd/MM/yyyy'),TO_TIMESTAMP(a.VENCIMIENTO, 'dd/MM/yyyy')) AS smallint) AS AMRT_TRM, 
    'D' AS COD_UNI_AMRT_TRM,   
    a.COSTO AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, NULL AS COD_TYP_LIC, 
    NULL AS COU_CAR_OFF, NULL AS COD_CONV, 
    NULL AS COD_EXEC_CTO, NULL AS COD_COVID_PORT, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.COMPRA, 'dd/MM/yyyy'), 'yyyyMMdd') AS DATE_DISB, 
    NULL AS CARD_NUMBER, NULL AS FIELD1_CTO, NULL AS FIELD2_CTO, NULL AS FIELD3_CTO, NULL AS FIELD4_CTO, 
    NULL AS FIELD5_CTO, NULL AS FIELD6_CTO, NULL AS FIELD7_CTO, NULL AS FIELD8_CTO, 
    NULL AS FIELD9_CTO, NULL AS FIELD10_CTO, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_EST_PORT_88 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

------------------- Estado de portafolio 118 y 119 -------------------
--Inclusión de MIS_LOAD_EST_POR.sql si aplica o copia de datos de día anterior
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT A.COD_CONT, A.IDF_CTO, A.COD_GL, A.DES_GL, A.COD_ACCO_CENT,  
    A.COD_OFFI, A.COD_BLCE_STATUS, A.COD_VALUE, A.COD_CURRENCY,  
    A.COD_ENTITY, A.COD_PRODUCT, A.COD_SUBPRODUCT, A.COD_ACT_TYPE, 
    A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, 
    A.COD_INFO_SOURCE 
FROM MIS_LOAD_AUX_EST_118_119 A
WHERE A.DATA_DATE = '${var:periodo}'
UNION ALL
SELECT A.COD_CONT, A.IDF_CTO, A.COD_GL, A.DES_GL, A.COD_ACCO_CENT,  
    A.COD_OFFI, A.COD_BLCE_STATUS, A.COD_VALUE, A.COD_CURRENCY,  
    A.COD_ENTITY, A.COD_PRODUCT, A.COD_SUBPRODUCT, A.COD_ACT_TYPE, 
    A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, 
    A.COD_INFO_SOURCE 
FROM MIS_APR_LIABILITIES A
WHERE DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) != 1 
    AND '${var:periodo}' != (SELECT MAX(DATA_DATE) DATA_DATE FROM MIS_LOAD_AUX_EST_118_119)
    AND A.COD_INFO_SOURCE IN ('ESTADO_PORTAFOLIO_118', 'ESTADO_PORTAFOLIO_119') 
    AND TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1)
;


--- Crear partición en APR si no existe ---
ALTER TABLE MIS_APR_LIABILITIES ADD IF NOT EXISTS PARTITION (DATA_DATE='${var:periodo}');