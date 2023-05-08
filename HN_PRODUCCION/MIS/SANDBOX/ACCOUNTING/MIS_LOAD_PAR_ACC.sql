------------------------------------------------ CREACIÓN TABLAS DE PARÁMETROS DE RELACIONES ------------------------------------------------ 
--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de la Tabla de Parametría Cuenta Contable/Agrupador Contable ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_CAF_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_CAF_ACC.csv' INTO TABLE MIS_PAR_REL_CAF_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_PL_ACC.csv' INTO TABLE MIS_PAR_REL_PL_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BP_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_BP_ACC.csv' INTO TABLE MIS_PAR_REL_BP_ACC;

--- Llenado de la Tabla de Parametría Producto Balance/Línea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_BL_ACC.csv' INTO TABLE MIS_PAR_REL_BL_ACC;

--- Llenado de la Tabla de Catálogo de Líneas de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BL;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_BL.csv' INTO TABLE MIS_PAR_CAT_BL;

--- Llenado de la tabla de Concepto Contable y Código de Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CAF;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_CAF.csv' INTO TABLE MIS_PAR_CAT_CAF;

--- Llenado de la Tabla de Catálogo de Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BP;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_BP.csv' INTO TABLE MIS_PAR_CAT_BP;

--- Llenado de la Tabla de Catálogo de Cuenta de Gestión ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PL;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_PL.csv' INTO TABLE MIS_PAR_CAT_PL;

--- Llenado de tabla de jerarquía de Producto de Balance ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BLCE_PROD; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_BLCE_PROD.csv' INTO TABLE MIS_HIERARCHY_BLCE_PROD;

--- Llenado de tabla de jerarquía de Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_PL_ACC.csv' INTO TABLE MIS_HIERARCHY_PL_ACC;

--- Llenado de tabla de jerarquía de Linea de negocio ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BL; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_BL.csv' INTO TABLE MIS_HIERARCHY_BL;

--- Llenado de tabla de jerarquía de Unidad de negocio ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_UN; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_UN.csv' INTO TABLE MIS_HIERARCHY_UN;

--- Llenado de tabla de nuevas dimensiones ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_REG_DIMENSIONS; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_REG_DIMENSIONS.csv' INTO TABLE MIS_PAR_REL_REG_DIMENSIONS;

--- Llenado de tabla de multilatino ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_MUL_LAT; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_MUL_LAT.csv' INTO TABLE MIS_PAR_CAT_MUL_LAT;

--- Llenado de tabla de catalogo grupo economico ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ECO_GRO; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_ECO_GRO.csv' INTO TABLE MIS_PAR_CAT_ECO_GRO;

--- Llenado de tabla de catalogo convenio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CONV; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_CONV.csv' INTO TABLE MIS_PAR_CAT_CONV;

--- Llenado de catalogo de centro de costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ACCO_CENT;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_ACCO_CENT.csv' INTO TABLE MIS_PAR_CAT_ACCO_CENT;