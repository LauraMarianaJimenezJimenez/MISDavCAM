--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Ajustes Manuales Operacionales ---

ALTER TABLE MIS_DWH_MANUAL_ADJ_OPER
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}')
;

ALTER TABLE MIS_DWH_MANUAL_ADJ_OPER
ADD IF NOT EXISTS PARTITION (DATA_DATE = '${var:periodo}');

LOAD DATA INPATH '${var:ruta_fuentes_ajustes}/Ajustes_Manuales_Operacionales.csv' INTO TABLE MIS_DWH_MANUAL_ADJ_OPER PARTITION (DATA_DATE = '${var:periodo}');