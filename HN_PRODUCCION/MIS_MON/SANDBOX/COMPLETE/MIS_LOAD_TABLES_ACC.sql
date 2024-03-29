--------------------------------------------------------------- ASIG. DIM. OPERACIONAL -----------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

TRUNCATE TABLE IF EXISTS MIS_LOAD_CFP31001; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CFP31001.csv' INTO TABLE MIS_LOAD_CFP31001;

TRUNCATE TABLE IF EXISTS MIS_LOAD_CFP21001; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CFP21001.csv' INTO TABLE MIS_LOAD_CFP21001;

TRUNCATE TABLE IF EXISTS MIS_LOAD_CFP10301;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/CFP10301.csv' INTO TABLE MIS_LOAD_CFP10301;

TRUNCATE TABLE IF EXISTS MIS_LOAD_CFP50301;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/CFP50301.csv' INTO TABLE MIS_LOAD_CFP50301;

TRUNCATE TABLE MIS_PAR_CAT_PRODUCT;

INSERT INTO MIS_PAR_CAT_PRODUCT
SELECT CASE WHEN CFADSG = '1' THEN 'AHO' ELSE 'CTE' END AS COD_CONT, CFTNBR, CFPRNM
FROM MIS_LOAD_CFP21001
UNION ALL
SELECT 'PLZ' AS COD_CONT, CFTYP, CFPRNM
FROM MIS_LOAD_CFP31001
UNION ALL
SELECT 'PREST' AS COD_CONT, CFTYP, CFLNME
FROM MIS_LOAD_CFP50301
;

TRUNCATE TABLE MIS_PAR_CAT_MANAGER;

INSERT INTO MIS_PAR_CAT_MANAGER
SELECT CFOFFN AS COD_MANAGER, CFOFNM AS DES_MANAGER 
FROM MIS_LOAD_CFP10301
WHERE CFBANK = '1'
;

----Inserción de registros en la tabla de dimensiones DWH_ACCOUNTING
ALTER TABLE MIS_DWH_${var:tabla}_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla}_M PARTITION (DATA_DATE)
SELECT *
FROM MIS_DWH_${var:tabla}
WHERE DATA_DATE = '${var:periodo}';