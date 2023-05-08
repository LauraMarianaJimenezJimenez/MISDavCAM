------------------------------------------------ MIS_APR_CLIENT_DT ------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_MIS_CLIENTES; 
LOAD DATA INPATH '${var:ruta_fuentes_clientes}/MIS_CLIENTES.CSV' INTO TABLE MIS_LOAD_MIS_CLIENTES;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_CLIENT_DT 
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Aprovisionamiento del Detalle del cliente
INSERT INTO MIS_APR_CLIENT_DT 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT a.EN_ENTE AS IDF_CLI, NULL AS TYP_DOC_ID, a.EN_CED_RUC AS NUM_DOC_ID,
    a.EN_NOMBRE AS NOM_NAME, CONCAT(a.P_P_APELLIDO, ' ', a.P_S_APELLIDO) AS NOM_LASTN,
    NULL AS COD_RES_COUNT, a.EN_SUBTIPO AS COD_TYP_CLNT, a.ESTADO_ENTE AS COD_COND_CLNT,
    NULL AS COD_ENG, NULL AS VAL_SEX, NULL AS COD_COUNTRY, NULL AS COD_SECTOR, NULL AS DES_SECTOR,
    CONCAT(a.EN_LINEA_NEG_CV, IF(b.IDF_CLI IS NOT NULL, '_INST', '')) AS COD_SEGMENT, 
    CONCAT(a.EN_LINEA_NEG_CV, IF(b.IDF_CLI IS NOT NULL, '_INST', '')) AS DES_SEGMENT, 
    a.EN_SUBSEGMENTO AS COD_SUBSEGMENT, a.EN_SUBSEGMENTO AS DES_SUBSEGMENT, NULL AS IND_EMPL_GRUP, 
    FROM_TIMESTAMP(TO_TIMESTAMP(a.EN_FECHA_CREA, 'MMM dd yyyy '), 'yyyyMMdd') AS DATE_CLNT_REG,
    NULL AS DATE_CLNT_WITHDRAW, a.EN_EJECUTIVO_CONTACTO AS COD_MANAGER, a.FU_NOMBRE AS DES_MANAGER,
    NULL AS RATING_CLI, NULL AS COD_GROUP_EC,
    NULL AS ID_ECO_GRO, NULL AS COU_ECO_GRO, NULL AS IND_MUL_LAT,
    NULL AS FIELD1_CLI, NULL AS FIELD2_CLI, NULL AS FIELD3_CLI, NULL AS FIELD4_CLI, 
    NULL AS FIELD5_CLI, NULL AS FIELD6_CLI, NULL AS FIELD7_CLI, NULL AS FIELD8_CLI, 
    NULL AS FIELD9_CLI, NULL AS FIELD10_CLI,
    NULL AS COD_SECTOR_LOCAL, NULL AS DES_SECTOR_LOCAL, NULL AS COD_SECTOR_REG, NULL AS DES_SECTOR_REG
FROM MIS_LOAD_MIS_CLIENTES a
LEFT JOIN (SELECT DISTINCT TRIM(IDF_CLI) IDF_CLI FROM MIS_PAR_CAT_INST_CLI) b
ON TRIM(a.EN_ENTE) = b.IDF_CLI;


--COMPUTE INCREMENTAL STATS MIS_APR_CLIENT_DT PARTITION (DATA_DATE = '${var:periodo}');