------------------------------------------------------------- MIS_APR_FEES -------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_FEES 
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Aprovisionamiento de Tarjetas Comisiones
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
--Se agrupan los saldos para consolidar los resultados por contrato del dia de ejecucion, y acumularlos con los resultados del día anterior
SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE,
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
    CAST(ROUND(SUM(a.PL), 10) AS decimal(30, 10)) AS PL, a.COD_INFO_SOURCE
FROM (
    --Comisiones por tarjeta registrados en el dia de ejecucion
    SELECT 'COM' AS COD_CONT, IFNULL(TRIM(d.TA_ID_OPERACION), TRIM(a.TARJETA)) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, --TODO
        NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
        CONCAT('COM_', TRIM(a.MOVIMIENTO)) AS COD_VALUE, 
        CASE TRIM(a.MONEDA)
            WHEN 'CO' THEN 'CRC'
            WHEN 'DO' THEN 'USD' 
            END AS COD_CURRENCY, '4' AS COD_ENTITY,
        COALESCE(TRIM(SPE.COD_PRODUCT), UPPER(TRIM(b.NOMBRE_BIN))) AS COD_PRODUCT, UPPER(TRIM(b.PRODUCTO)) AS COD_SUBPRODUCT, 
        NULL AS COD_ACT_TYPE, 
        (a.MONTO * c.SIGNO * -1) AS PL, 'MV' AS COD_INFO_SOURCE
    FROM MIS_TMP_LOAD_MV_ASS a
    LEFT JOIN MIS_LOAD_INFO_TRABAJO b 
    ON STRLEFT(TRIM(a.TARJETA), 6) = TRIM(b.BIN)
    LEFT JOIN MIS_LOAD_TCS d
    ON TRIM(a.TARJETA) = TRIM(d.TA_NUM_TARJETA_TITULAR) AND TRIM(d.TA_INDICA_INTRA_EXTRA) = ''
    AND CASE TRIM(a.MONEDA)
        WHEN 'CO' THEN 'CRC'
        WHEN 'DO' THEN 'USD' 
        END =
        CASE TRIM(d.TA_TMONEDA) 
        WHEN '1' THEN 'CRC' 
        WHEN '2' THEN 'USD' 
        END
    INNER JOIN MIS_LOAD_LISTA_MOV c
    ON TRIM(a.MOVIMIENTO) = TRIM(c.COD) AND TRIM(c.COD) IN ('29', 'C0', 'MO', 'RH', 'RU', 'CE', 'UB')
    LEFT JOIN MIS_PAR_REL_PROD_SPE SPE 
    ON TRIM(SPE.IDF_CTO) = IFNULL(TRIM(d.TA_ID_OPERACION), TRIM(a.TARJETA))
    UNION ALL
    --Comisiones por tarjeta registrados en el dia anterior
    SELECT a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
        a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, 
        a.PL, a.COD_INFO_SOURCE
    FROM MIS_APR_FEES a
    WHERE a.COD_INFO_SOURCE = 'MV' AND  
        DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) > 1 AND 
        TO_TIMESTAMP(a.DATA_DATE, 'yyyyMMdd') = DAYS_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1) 
) AS a 
GROUP BY a.COD_CONT, a.IDF_CTO, a.COD_GL, a.DES_GL, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, 
    a.COD_VALUE, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.COD_ACT_TYPE, a.COD_INFO_SOURCE; 


--- Crear partición en APR si no existe ---
ALTER TABLE MIS_APR_FEES ADD IF NOT EXISTS PARTITION (DATA_DATE='${var:periodo}');