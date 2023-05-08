------------------------------------------------ MOTOR DE PARAMETRÍA DE FLUJO DE ACTIVOS --------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Parametría de Motor de Repartos de Gastos por Centro de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de Tabla de Parametría de Motor de Repartos de Gastos por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de Tabla de Parametría de Driver de Repartos de Gastos por Centro de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de Tabla de Parametría de Driver de Repartos de Gastos por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

--- Llenado de Tablas de Parametrías de Motor de Repartos Internegocios ---
TRUNCATE TABLE MIS_PAR_INTER_SEG_DRI;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

TRUNCATE TABLE MIS_PAR_INTER_SEG_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_complete}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;