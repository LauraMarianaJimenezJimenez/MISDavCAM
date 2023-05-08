--------------------------------------------------------------- ASIG. DIM. OPERACIONAL -----------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Inserción de registros en la tabla de dimensiones DWH_ASSETS
ALTER TABLE MIS_DWH_${var:tabla}_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla}_M PARTITION (DATA_DATE)
SELECT *
FROM MIS_DWH_${var:tabla}
WHERE DATA_DATE = '${var:periodo}';