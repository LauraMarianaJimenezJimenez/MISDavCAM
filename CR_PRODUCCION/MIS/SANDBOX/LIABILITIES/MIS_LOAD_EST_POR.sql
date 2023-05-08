------------------------------------------------------------- MIS_APR_LIABILITIES ---------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_EST_PORT_118; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ESTADO_PORTAFOLIO_118.CSV' INTO TABLE MIS_LOAD_EST_PORT_118;
TRUNCATE TABLE IF EXISTS MIS_LOAD_EST_PORT_119; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ESTADO_PORTAFOLIO_119.CSV' INTO TABLE MIS_LOAD_EST_PORT_119;

--Limpieza de tabla auxiliar, luego es cargada en MIS_APR_LIABILITIES.sql
TRUNCATE TABLE IF EXISTS MIS_LOAD_AUX_EST_118_119;

------------------- Estado de portafolio 118 -------------------

----Aprovisionamiento de Bonos y Papeles Colones
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.FACIAL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_118' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_118 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Primas por Bonos y Papeles Colones
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'PRI_BO' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * (a.COSTO-a.FACIAL) AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_118' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_118 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Primas y Descuentos por Bonos y Papeles Colones
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, IF(a.ACU_DES_PRI < 0, 'PRI', 'DES') AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.ACU_DES_PRI AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_118' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_118 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Intereses por Bonos y Papeles Colones
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.INT_ACUMUL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_118' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_118 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;


----Aprovisionamiento de Bonos y Papeles Colones
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    CAST(a.INT_DIA AS DECIMAL(30,10)) AS PL, 'ESTADO_PORTAFOLIO_118' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_118 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;


--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_118');

----Aprovisionamiento de Activos Contrato
INSERT INTO MIS_APR_CONTRACT_DT
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_118') 
SELECT 
    TRIM(a.INVERSION) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, 
    NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY,
    TRIM(a.EMISOR) AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE,
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
FROM MIS_LOAD_EST_PORT_118 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

------------------- Estado de portafolio 119 -------------------

----Aprovisionamiento de Bonos y Papeles Dolares
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.FACIAL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_119' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_119 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Primas por Bonos y Papeles Dolares
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'PRI_BO' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * (a.COSTO-a.FACIAL) AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_119' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_119 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Primas y Descuentos por Bonos y Papeles Dolares
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, IF(a.ACU_DES_PRI < 0, 'PRI', 'DES') AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.ACU_DES_PRI AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_119' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_119 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;

----Aprovisionamiento de Intereses por Bonos y Papeles Dolares
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    CAST(-1 * a.INT_ACUMUL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'ESTADO_PORTAFOLIO_119' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_119 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;


----Aprovisionamiento de Bonos y Papeles Dolares
INSERT INTO MIS_LOAD_AUX_EST_118_119 
SELECT 'CTA' AS COD_CONT, TRIM(a.INVERSION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL,
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE,
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE,
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    CAST(a.INT_DIA AS DECIMAL(30,10)) AS PL, 'ESTADO_PORTAFOLIO_119' AS COD_INFO_SOURCE, '${var:periodo}' AS DATA_DATE
FROM MIS_LOAD_EST_PORT_119 a 
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;


--- Limpieza de partición ----
ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_119');

----Aprovisionamiento de Activos Contrato
INSERT INTO MIS_APR_CONTRACT_DT
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ESTADO_PORTAFOLIO_119') 
SELECT 
    TRIM(a.INVERSION) AS IDF_CTO, '4' AS COD_ENTITY, 
    COALESCE(TRIM(SPE.COD_PRODUCT), TRIM(a.COD)) AS COD_PRODUCT, TRIM(a.INSTRUMENTO) AS COD_SUBPRODUCT, 
    NULL AS COD_ACT_TYPE, 
    CASE TRIM(a.MONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY,
    TRIM(a.EMISOR) AS IDF_CLI, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI,
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE,
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
FROM MIS_LOAD_EST_PORT_119 a
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.INVERSION)
;