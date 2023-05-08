----------------------------------------------------------- MOTOR DE REPARTOS INTERNEGOCIOS -----------------------------------------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Ajuste de porcentaje para totalizar drivers de Segmento-Producto a 100% ---
TRUNCATE TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG_DRI;
DROP TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG_DRI;
CREATE TABLE MIS_TMP_PAR_INTER_SEG_DRI AS 
SELECT a.COD_DRIVER, a.COD_SEGMENT, IF(TRIM(a.COD_BLCE_PROD)!='', a.COD_BLCE_PROD, NULL) AS COD_BLCE_PROD, 
    CAST(a.ALLOCATION_PERC /TMP.TOT_PERC AS decimal(30, 10)) AS ALLOCATION_PERC   
FROM MIS_PAR_INTER_SEG_DRI a
LEFT JOIN (SELECT TMP.COD_DRIVER, SUM(TMP.ALLOCATION_PERC ) AS TOT_PERC FROM MIS_PAR_INTER_SEG_DRI TMP GROUP BY TMP.COD_DRIVER) TMP
ON a.COD_DRIVER = TMP.COD_DRIVER;


------------ Creación de la tabla Repartos por Segmento-Producto con Descripciones ------------
TRUNCATE TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG;
DROP TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG;
CREATE TABLE MIS_TMP_PAR_INTER_SEG AS
SELECT DISTINCT 
    a.*, b.COD_SEGMENT AS COD_SEGMENT_DESTINY, b.COD_BLCE_PROD AS COD_BLCE_PROD_DESTINY, b.ALLOCATION_PERC , 
    b.COD_SEGMENT AS DES_SEGMENT_DESTINY, --TODO: PREGUNTAR
    d.DES_BLCE_PROD AS DES_BLCE_PROD_DESTINY
FROM MIS_PAR_INTER_SEG_ENG a
INNER JOIN MIS_TMP_PAR_INTER_SEG_DRI b 
ON a.COD_DRIVER = b.COD_DRIVER 
--LEFT JOIN MIS_PAR_CAT_BL c --TODO: PREGUNTAR SI VA DESCRIPCION Y TABLA
--ON b.COD_SEGMENT = c.COD_SEGMENT
LEFT JOIN MIS_PAR_CAT_BP d 
ON b.COD_BLCE_PROD = d.COD_BLCE_PROD;


------------ Suma de valor de contratos y cálculo de repartos por Línea de Negocio ------------

----Agrupación de cuentas a repartir
TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_STG_INI;
DROP TABLE IF EXISTS MIS_TMP_INTER_STG_INI;
CREATE TABLE MIS_TMP_INTER_STG_INI AS 
SELECT DWH.DATA_DATE, DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, CAST(SUM(DWH.PL) AS decimal(30, 10)) AS PL, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_SEGMENT, DWH.DES_SEGMENT, DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE
FROM MIS_DWH_RECONCILIATION DWH
WHERE DWH.DATA_DATE='${var:periodo}' AND IFNULL(DWH.PL, 0)<>0 AND DWH.COD_CONT IN ('RCTB', 'CTBP', 'RCCL')
GROUP BY DWH.DATA_DATE, DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_SEGMENT, DWH.DES_SEGMENT, DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE;


----------------- Reparto por segmento y producto balance -----------------

----Cálculo del Reparto por segmento y producto balance
TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_1; 
DROP TABLE IF EXISTS MIS_TMP_INTER_1; 
CREATE TABLE MIS_TMP_INTER_1 AS 
SELECT DWH.DATA_DATE, DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, 
    COALESCE(AL1.ALLOCATION_PERC , AL2.ALLOCATION_PERC , AL3.ALLOCATION_PERC , 0) * DWH.PL AS PL,
    COALESCE(AL1.ALLOCATION_PERC , AL2.ALLOCATION_PERC , AL3.ALLOCATION_PERC , 0) * DWH.PL * -1 AS PL_ORIGIN, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, 
    COALESCE(AL1.COD_SEGMENT_DESTINY, AL2.COD_SEGMENT_DESTINY, AL3.COD_SEGMENT_DESTINY, DWH.COD_SEGMENT) AS COD_SEGMENT, 
    DWH.COD_SEGMENT AS COD_SEGMENT_ORIGIN,
    COALESCE(AL1.DES_SEGMENT_DESTINY, AL2.DES_SEGMENT_DESTINY, AL3.DES_SEGMENT_DESTINY, DWH.DES_SEGMENT) AS DES_SEGMENT, 
    DWH.DES_SEGMENT AS DES_SEGMENT_ORIGIN, 
    COALESCE(AL1.COD_BLCE_PROD_DESTINY, AL2.COD_BLCE_PROD_DESTINY, AL3.COD_BLCE_PROD_DESTINY, DWH.COD_BLCE_PROD) AS COD_BLCE_PROD, 
    DWH.COD_BLCE_PROD AS COD_BLCE_PROD_ORIGIN,
    COALESCE(AL1.DES_BLCE_PROD_DESTINY, AL2.DES_BLCE_PROD_DESTINY, AL3.DES_BLCE_PROD_DESTINY, DWH.DES_BLCE_PROD) AS DES_BLCE_PROD, 
    DWH.DES_BLCE_PROD AS DES_BLCE_PROD_ORIGIN,
    IF(COALESCE(AL1.COD_DRIVER, AL2.COD_DRIVER, AL3.COD_DRIVER) IS NOT NULL, 'Y', 'N') AS SEG_INTER
FROM MIS_TMP_INTER_STG_INI DWH
LEFT JOIN MIS_TMP_PAR_INTER_SEG AL1 --Reparto COD_GL_GROUP - COD_ACCO_CENT
ON DWH.COD_ENTITY=AL1.COD_ENTITY AND DWH.COD_CURRENCY=AL1.COD_CURRENCY AND DWH.COD_GL_GROUP=AL1.COD_GL_GROUP AND DWH.COD_ACCO_CENT=AL1.COD_ACCO_CENT
LEFT JOIN MIS_TMP_PAR_INTER_SEG AL2 --Si no encontró COD_GL_GROUP - COD_ACCO_CENT, Reparto únicamente por COD_GL_GROUP
ON COALESCE(AL1.COD_DRIVER) IS NULL AND 
   DWH.COD_ENTITY=AL2.COD_ENTITY AND DWH.COD_CURRENCY=AL2.COD_CURRENCY AND DWH.COD_GL_GROUP=AL2.COD_GL_GROUP AND TRIM(AL2.COD_ACCO_CENT) = ''
LEFT JOIN MIS_TMP_PAR_INTER_SEG AL3 --Si no encontró COD_GL_GROUP - COD_ACCO_CENT o COD_GL_GROUP, Reparto únicamente por COD_ACCO_CENT
ON COALESCE(AL1.COD_DRIVER, AL2.COD_DRIVER) IS NULL AND 
   DWH.COD_ENTITY=AL3.COD_ENTITY AND DWH.COD_CURRENCY=AL3.COD_CURRENCY AND  TRIM(AL3.COD_GL_GROUP) = '' AND DWH.COD_ACCO_CENT=AL3.COD_ACCO_CENT;

------------ Limpieza de partición ------------
ALTER TABLE MIS_DWH_RECONCILIATION
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_CONT = 'RCIN');

----Inserción de registros repartidos por segmento y producto balance
INSERT INTO MIS_DWH_RECONCILIATION 
    (COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, PL, COD_INFO_SOURCE, 
    COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, 
    COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, COD_SEGMENT, DES_SEGMENT) ---TODO: PREGUNTAR DES_SEGMENT
PARTITION (DATA_DATE, COD_CONT)
SELECT DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, 
    CAST(SUM(DWH.PL) AS decimal(30, 10)) AS PL, 'RCIN' AS COD_INFO_SOURCE, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, HIE.COD_BUSINESS_LINE, CAT.DES_BUSINESS_LINE, 
    DWH.COD_SEGMENT, DWH.DES_SEGMENT, DWH.DATA_DATE, 'RCIN' AS COD_CONT 
FROM MIS_TMP_INTER_1 DWH
LEFT JOIN MIS_HIERARCHY_BL HIE
ON TRIM(DWH.COD_SEGMENT) = TRIM(HIE.COD_SEGMENT)
LEFT JOIN MIS_PAR_CAT_BL CAT
ON TRIM(HIE.COD_BUSINESS_LINE) = TRIM(CAT.COD_BUSINESS_LINE)
WHERE DWH.SEG_INTER = 'Y'
GROUP BY DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY,  
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, HIE.COD_BUSINESS_LINE, CAT.DES_BUSINESS_LINE, 
    DWH.COD_SEGMENT, DWH.DES_SEGMENT, DWH.DATA_DATE;

----Inserción de contrapartida de registros repartidos por segmento y producto balance
INSERT INTO MIS_DWH_RECONCILIATION 
    (COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, PL, COD_INFO_SOURCE, 
    COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, 
    COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, COD_SEGMENT, DES_SEGMENT) 
PARTITION (DATA_DATE, COD_CONT)
SELECT DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY, 
    CAST(SUM(DWH.PL_ORIGIN) AS decimal(30, 10)) AS PL, 'RCIN' AS COD_INFO_SOURCE, 
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_BLCE_PROD_ORIGIN, DWH.DES_BLCE_PROD_ORIGIN, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, 
    DWH.COD_SEGMENT_ORIGIN, DWH.DES_SEGMENT_ORIGIN, DWH.DATA_DATE, 'RCCL' AS COD_CONT 
FROM MIS_TMP_INTER_1 DWH 
WHERE DWH.SEG_INTER = 'Y'
GROUP BY DWH.COD_ACCO_CENT, DWH.COD_BLCE_STATUS, DWH.COD_CURRENCY, DWH.COD_ENTITY,  
    DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, 
    DWH.COD_BLCE_PROD_ORIGIN, DWH.DES_BLCE_PROD_ORIGIN, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, 
    DWH.COD_SEGMENT_ORIGIN, DWH.DES_SEGMENT_ORIGIN, DWH.DATA_DATE;


----Eliminación de tablas temporales
--TRUNCATE TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG_DRI;
--DROP TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG_DRI;
--TRUNCATE TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG;
--DROP TABLE IF EXISTS MIS_TMP_PAR_INTER_SEG;
--TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_STG_INI;
--DROP TABLE IF EXISTS MIS_TMP_INTER_STG_INI;
--TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_1;
--DROP TABLE IF EXISTS MIS_TMP_INTER_1;


----------------- Validación de las tablas paramétricas del motor -----------------

----Tabla temporal de validaciones a realizar
DROP TABLE IF EXISTS MIS_TMP_INTER_DES_VALIDATION;
CREATE TABLE MIS_TMP_INTER_DES_VALIDATION (
COD_VALIDATION STRING, DES_VALIDATION STRING); 

----Tabla temporal para almacenar resultados
DROP TABLE IF EXISTS MIS_TMP_INTER_VALIDATION;
CREATE TABLE MIS_TMP_INTER_VALIDATION (
COD_DRIVER STRING, DIMENSION STRING, COD_VALIDATION STRING);

----Descripción de validaciones a realizar
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('001', 'El porcentaje total repartido no es igual a 1 (100%)');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('002', 'Hay porcentajes de repartición fuera del rango definido (0 a 1)');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('003', 'Hay Centros de Costo sin parametrizar en la tabla MIS_PAR_REL_EXP_TYP');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('004', 'Hay Segmentos sin parametrizar en las tabla MIS_PAR_REL_SEGMENT');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('005', 'Hay Productos de Balance sin parametrizar en la tabla MIS_PAR_REL_BP');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('006', 'El driver no fue parametrizado en la tabla de drivers MIS_PAR_INTER_AC_DRI');
INSERT INTO MIS_TMP_INTER_DES_VALIDATION VALUES ('007', 'El driver no fue parametrizado en la tabla de drivers MIS_PAR_INTER_SEG_DRI');


----Validación de porcentaje total a repartir por Segmento-Producto
INSERT INTO MIS_TMP_INTER_VALIDATION
SELECT a.COD_DRIVER, 'SEGMENTO - PRODUCTO' AS DIMENSION, '001' AS COD_VALIDATION
FROM MIS_PAR_INTER_SEG_DRI a
GROUP BY a.COD_DRIVER
HAVING SUM(a.ALLOCATION_PERC )  <> 1;

----Validación de porcentajes a repartir por Segmento-Producto para descartar porcentaje negativos o mayores a 1
INSERT INTO MIS_TMP_INTER_VALIDATION
SELECT a.COD_DRIVER, 'SEGMENTO - PRODUCTO' AS DIMENSION, '002' AS COD_VALIDATION
FROM MIS_PAR_INTER_SEG_DRI a
GROUP BY a.COD_DRIVER
HAVING SUM(IF(a.ALLOCATION_PERC  BETWEEN 0 AND 1, 0, 1)) <> 0;

----Verificación del segmento del Motor de Repartos 
/*INSERT INTO MIS_TMP_INTER_VALIDATION --TODO: EXCLUSION DE CODIGO O DEFINICION DE PAR_REL_SEGMENTs
SELECT a.COD_DRIVER, 'SEGMENTO - PRODUCTO' AS DIMENSION, '004' AS COD_VALIDATION
FROM MIS_PAR_INTER_SEG_DRI a
LEFT JOIN (
    SELECT DISTINCT A.COD_SEGMENT --TODO: PREGUNTAR
    FROM MIS_PAR_REL_SEGMENT A) b
ON a.COD_SEGMENT = b.COD_SEGMENT 
WHERE TRIM(a.COD_SEGMENT) <> '' AND b.COD_SEGMENT IS NULL
GROUP BY a.COD_DRIVER;*/

----Verificación de Producto Balance del Motor de Repartos de Gastos 
INSERT INTO MIS_TMP_INTER_VALIDATION
SELECT a.COD_DRIVER, 'SEGMENTO - PRODUCTO' AS DIMENSION, '005' AS COD_VALIDATION
FROM MIS_PAR_INTER_SEG_DRI a 
LEFT JOIN (
    SELECT DISTINCT COD_BLCE_PROD
    FROM MIS_PAR_REL_BP_ACC
    UNION
    SELECT DISTINCT COD_BLCE_PROD
    FROM MIS_PAR_REL_BP_OPER) b
ON a.COD_BLCE_PROD = b.COD_BLCE_PROD
WHERE TRIM(a.COD_BLCE_PROD) <> '' AND b.COD_BLCE_PROD IS NULL
GROUP BY a.COD_DRIVER;

----Validacion de driver parametrizado para Segmento-Producto
INSERT INTO MIS_TMP_INTER_VALIDATION
SELECT a.COD_DRIVER, 'SEGMENTO - PRODUCTO' AS DIMENSION, '007' AS COD_VALIDATION
FROM MIS_PAR_INTER_SEG_ENG a 
LEFT JOIN MIS_PAR_INTER_SEG_DRI b
ON a.COD_DRIVER = b.COD_DRIVER 
WHERE b.COD_DRIVER IS NULL
GROUP BY a.COD_DRIVER;


----Llenado de tabla final de validaciones del motor
TRUNCATE TABLE IF EXISTS MIS_VAL_ALLOCATIONS;
INSERT INTO MIS_VAL_ALLOCATIONS
SELECT DISTINCT '${var:periodo}' AS DATA_DATE, a.DIMENSION, a.COD_DRIVER, b.DES_VALIDATION
FROM MIS_TMP_INTER_VALIDATION a
LEFT JOIN MIS_TMP_INTER_DES_VALIDATION b
ON a.COD_VALIDATION = b.COD_VALIDATION;

----Eliminación de tablas temporales
TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_DES_VALIDATION;
DROP TABLE IF EXISTS MIS_TMP_INTER_DES_VALIDATION;
TRUNCATE TABLE IF EXISTS MIS_TMP_INTER_VALIDATION;
DROP TABLE IF EXISTS MIS_TMP_INTER_VALIDATION;