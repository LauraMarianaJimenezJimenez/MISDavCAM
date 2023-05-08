------------------------------------------------------------- MIS_APR_OFF_BALANCE -------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_IDNV; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/IDNV.CSV' INTO TABLE MIS_LOAD_IDNV;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_ASSETS_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--Fuentes cargadas antes de 05/21: ta_saldo_principal es ajuste_de_intereses por error de TI

----Aprovisionamiento de Devengo de Intereses para Tarjetas 
INSERT INTO MIS_APR_ASSETS_M 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, TRIM(a.TA_ID_OPERACION) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, --TODO
    NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, 
    CASE TRIM(a.DESCRIPCION_MORA)
      WHEN 'VIGENTE' THEN '1'
      WHEN 'VENCIDA' THEN '2'  
    END AS COD_BLCE_STATUS, 'INT' AS COD_VALUE, 
    CASE TRIM(a.TA_TMONEDA)
        WHEN '1' THEN 'CRC'
        WHEN '2' THEN 'USD' 
        END AS COD_CURRENCY, '4' AS COD_ENTITY,
    COALESCE(TRIM(SPE.COD_PRODUCT), CONCAT(UPPER(TRIM(b.NOMBRE_BIN)), IF(TRIM(a.TA_INDICA_INTRA_EXTRA)!='', CONCAT('_', TRIM(a.TA_INDICA_INTRA_EXTRA)), ''))) AS COD_PRODUCT,
    UPPER(TRIM(b.PRODUCTO)) AS COD_SUBPRODUCT, NULL AS COD_ACT_TYPE, 
    (a.AJUSTE_DE_INTERESES) AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    NULL AS PL, 'IDNV' AS COD_INFO_SOURCE
FROM MIS_LOAD_IDNV a 
LEFT JOIN MIS_LOAD_INFO_TRABAJO b 
ON STRLEFT(TRIM(a.TA_NUM_TARJETA_TITULAR), 6) = TRIM(b.BIN)
LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
ON TRIM(SPE.IDF_CTO) = TRIM(a.TA_ID_OPERACION)
;