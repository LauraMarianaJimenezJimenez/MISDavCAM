--------------------------------------------------------------- MIS_APR_EXPENSES ---------------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;
/*
----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_CONT_PROV; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/CONTABLE.csv' INTO TABLE MIS_LOAD_CONT_PROV;

TRUNCATE TABLE IF EXISTS MIS_LOAD_PROVEEDORES; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/PROVEEDORES.csv' INTO TABLE MIS_LOAD_PROVEEDORES;
*/
--- Limpieza de partición ----
ALTER TABLE MIS_APR_EXPENSES 
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');
/*
--- Crear partición en APR si no existe ---
ALTER TABLE MIS_APR_EXPENSES ADD IF NOT EXISTS PARTITION (DATA_DATE='${var:periodo}');

INSERT INTO MIS_APR_EXPENSES 
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'EXP' AS COD_CONT, RPAD(TRIM(a.CUENTA_MAYOR), 16, '0') AS COD_GL, NULL AS DES_GL, 
    TRIM(a.CENTRO_COSTO) AS COD_ACCO_CENT, TRIM(b.NOMBRE) AS COD_EXPENSE, TRIM(a.SUCURSAL) AS COD_OFFI, CAST(a.PROVEEDOR AS STRING) AS COD_NAR, NULL AS COD_BLCE_STATUS, a.MONEDA AS COD_CURRENCY, TRIM(a.COMPANIA) AS COD_ENTITY, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    CAST(a.MONTO AS decimal(30, 10)) AS PL, 'CONTABLE' AS COD_INFO_SOURCE
FROM MIS_LOAD_CONT_PROV a
LEFT JOIN MIS_LOAD_PROVEEDORES b
ON a.PROVEEDOR = b.PROVEEDOR
WHERE STRLEFT(TRIM(a.CUENTA_MAYOR),1) = '3' and from_timestamp(to_timestamp(fecha,'yyyy-MM-dd'),'yyyyMMdd') = '${var:periodo}'
;
*/