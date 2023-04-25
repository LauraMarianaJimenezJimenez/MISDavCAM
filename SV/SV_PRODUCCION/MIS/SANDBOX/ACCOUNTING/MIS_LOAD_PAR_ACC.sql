------------------------------------------------ CREACIÓN TABLAS DE PARÁMETROS DE RELACIONES ------------------------------------------------ 

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de la Tabla de Parametría Cuenta Contable/Agrupador Contable ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_CAF_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_CAF_ACC.csv' INTO TABLE MIS_PAR_REL_CAF_ACC;

/*
--- Llenado de la Tabla de Parametría Cuenta Contable/Agrupador Contable ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CAF; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_CAF.csv' INTO TABLE MIS_PAR_CAT_CAF;
*/

--- Llenado de la Tabla de Parametría Agrupador Contable/Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_PL_ACC.csv' INTO TABLE MIS_PAR_REL_PL_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PL; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_PL.csv' INTO TABLE MIS_PAR_CAT_PL;

--- Llenado de la Tabla de Parametría Agrupador Contable/Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BP_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_BP_ACC.csv' INTO TABLE MIS_PAR_REL_BP_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BP; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_BP.csv' INTO TABLE MIS_PAR_CAT_BP;

--- Llenado de la Tabla de Parametría Producto Balance/Línea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_BL_ACC.csv' INTO TABLE MIS_PAR_REL_BL_ACC;

--- Llenado de la Tabla de Catálogo de Líneas de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BL;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_BL.csv' INTO TABLE MIS_PAR_CAT_BL;

--- Llenado de la Tabla de Catálogo Catálogo de Sucursales Manuales ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_OFFI_MANUAL;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_OFFI_MANUAL.csv' INTO TABLE MIS_PAR_CAT_OFFI_MANUAL;

--- Llenado de tabla de jerarquía de Producto de Balance ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BLCE_PROD; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_BLCE_PROD.csv' INTO TABLE MIS_HIERARCHY_BLCE_PROD;

--- Llenado de tabla de jerarquía de Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_PL_ACC.csv' INTO TABLE MIS_HIERARCHY_PL_ACC;

--- Llenado de tabla de jerarquía de Producto de Balance Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BLCE_PROD_LC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_BLCE_PROD_LC.csv' INTO TABLE MIS_HIERARCHY_BLCE_PROD_LC;

--- Llenado de tabla de jerarquía de Cuenta P&G Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PL_ACC_LC; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_PL_ACC_LC.csv' INTO TABLE MIS_HIERARCHY_PL_ACC_LC;

--- Llenado de tabla de jerarquía de Linea de negocio Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BL; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_BL.csv' INTO TABLE MIS_HIERARCHY_BL;

--- Llenado de tabla de jerarquía de Unidad de negocio Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_UN; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_HIERARCHY_UN.csv' INTO TABLE MIS_HIERARCHY_UN;

--- Llenado de la Tabla de Catálogo de Sucursales ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_OFFI;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_OFFI.csv' INTO TABLE MIS_PAR_CAT_OFFI;

--- Llenado de la Tabla de Catálogo de Sector Economico ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_SECTOR_ECO;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_SECTOR_ECO.csv' INTO TABLE MIS_PAR_CAT_SECTOR_ECO;

--- Llenado de la Tabla de Catálogo de Código Gastos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_EXPENSE;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_CAT_EXPENSE.csv' INTO TABLE MIS_PAR_CAT_EXPENSE;

--- Llenado de la Tabla de Parametría impuestos ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_TAX_ENG;
--LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_TAX_ENG.csv' INTO TABLE MIS_PAR_TAX_ENG;

--- Llenado de la Tabla de Parametría Provisiones ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PROV_GL;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_PAR_REL_PROV_GL.csv' INTO TABLE MIS_PAR_REL_PROV_GL;


/*
--- Tabla extra: MIS_LOAD_VPSPLASTICO ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_VPSPLAST;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/MIS_LOAD_VPSPLAST.csv' INTO TABLE MIS_LOAD_VPSPLAST;
*/