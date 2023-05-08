------------------------------------------------ CREACIÓN TABLAS DE PARÁMETROS DE RELACIONES ------------------------------------------------ 

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de la Tabla de Parametría Reparto de Gastos por Centro de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto de Gastos por Centro de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de la Tabla de Parametría Reparto de Gastos por Segmento/Producto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto de Gastos por Segmento/Producto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

--- Llenado de la Tabla de Parametría Reparto Internegocios/Comisiones por Segmento/Producto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto Internegocios/Comisiones por Segmento/Producto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

--- Llenado de la Tabla de Parametría Reparto del balance por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_BALAN_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_BALAN_SEG_ENG.csv' INTO TABLE MIS_PAR_BALAN_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto del Balance por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_BALAN_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_BALAN_SEG_DRI.csv' INTO TABLE MIS_PAR_BALAN_SEG_DRI;

--- Llenado de la Tabla de Parametría del Motor de Impuestos---
TRUNCATE TABLE IF EXISTS MIS_PAR_TAX_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_TAX_ENG.csv' INTO TABLE MIS_PAR_TAX_ENG;