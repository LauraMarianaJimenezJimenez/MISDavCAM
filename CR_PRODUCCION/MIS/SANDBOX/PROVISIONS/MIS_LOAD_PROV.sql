------------------------------------------------------------- MIS_APR_PROVISIONS -------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
--TRUNCATE TABLE IF EXISTS MIS_LOAD_XYZ; 
--LOAD DATA INPATH '${var:ruta_fuentes_provisiones}/XYZ.CSV' INTO TABLE MIS_LOAD_XYZ;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_PROVISIONS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Crear partición en APR si no existe ---
ALTER TABLE MIS_APR_PROVISIONS ADD IF NOT EXISTS PARTITION (DATA_DATE='${var:periodo}');