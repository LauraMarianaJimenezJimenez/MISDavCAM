--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Dimensiones Regionales Intermedia ---
ALTER TABLE MIS_PAR_REL_REG_DIMENSIONS_COD_CONV 
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

ALTER TABLE MIS_PAR_REL_REG_DIMENSIONS_COD_CONV 
ADD IF NOT EXISTS PARTITION (DATA_DATE = '${var:periodo}');

LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_REL_REG_DIMENSIONS_COD_CONV.csv' INTO TABLE MIS_PAR_REL_REG_DIMENSIONS_COD_CONV 
PARTITION (DATA_DATE = '${var:periodo}');

--- Llenado de Tabla de Parametría Limpieza de Centros de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de Tabla de Parametría Motor de Gastos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de Tabla de Drivers Limpieza de Centros de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de Tabla de Drivers Motor de Gastos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

--- Llenado de Tabla de Parametría Motor de Internegocios (Comisiones) ---
TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;

TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

--- Llenado de la Tabla de Parametría impuestos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_TAX_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_TAX_ENG.csv' INTO TABLE MIS_PAR_TAX_ENG;

--- Inserción de Información en tabla final de dimensiones regionales ---

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_REG_DIMENSIONS; 
LOAD DATA INPATH '${var:ruta_fuentes_operacionales}/MIS_PAR_REL_REG_DIMENSIONS.csv' INTO TABLE MIS_PAR_REL_REG_DIMENSIONS;

INSERT INTO MIS_PAR_REL_REG_DIMENSIONS (IDF_CTO, COD_CONV)
SELECT IDF_CTO, COD_CONV
FROM MIS_PAR_REL_REG_DIMENSIONS_COD_CONV;