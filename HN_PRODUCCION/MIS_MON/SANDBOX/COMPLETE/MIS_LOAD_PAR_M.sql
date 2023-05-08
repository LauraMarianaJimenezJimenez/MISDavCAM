------------------------------------------------ MOTOR DE PARAMETRÍA DE FLUJO DE ACTIVOS --------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Parametría de Motor de Repartos de Gastos por Línea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de Tabla de Parametría de Motor de Repartos de Gastos por Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de Tabla de Parametría de Driver de Repartos de Gastos por Línea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de Tabla de Parametría de Driver de Repartos de Gastos por Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

--- Llenado de Tabla de Parametría de Motor de Repartos de internegocios Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;

--- Llenado de Tabla de Parametría de Driver de Repartos de internegocios por Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

--- Llenado de Tabla de parametria de impuestos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_TAX_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_TAX_ENG.csv' INTO TABLE MIS_PAR_TAX_ENG;