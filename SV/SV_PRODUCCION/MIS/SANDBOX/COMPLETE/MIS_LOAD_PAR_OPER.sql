------------------------------------------------ CREACIÓN TABLAS DE PARÁMETROS DE RELACIONES ----------------------------------------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de la Tabla de Parametría Asignación Subproductos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_SUBPROD_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_SUBPROD_OPER.csv' INTO TABLE MIS_PAR_REL_SUBPROD_OPER;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_CAF_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_CAF_OPER.csv' INTO TABLE MIS_PAR_REL_CAF_OPER;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BL_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_BL_OPER.csv' INTO TABLE MIS_PAR_REL_BL_OPER;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BP_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_BP_OPER.csv' INTO TABLE MIS_PAR_REL_BP_OPER;

--- Llenado de Tabla de Parametría de Motor de TTI ---
TRUNCATE TABLE MIS_PAR_TTI_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_TTI_ENG.csv' INTO TABLE MIS_PAR_TTI_ENG;

--- Llenado de Tabla de Contratos con TTI especial ---
TRUNCATE TABLE MIS_LOAD_TTI_SPE;
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_TTI_SPE.csv' INTO TABLE MIS_LOAD_TTI_SPE;

--- Llenado de Tabla de Parametría Motor de Internegocios (Comisiones) ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;

--TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

--- Llenado de Tabla de Parametría Limpieza de Centros de Costo ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de Tabla de Parametría Motor de Gastos ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de Tabla de Drivers Limpieza de Centros de Costo ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de Tabla de Drivers Motor de Gastos ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_EXP_TYP; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_EXP_TYP.csv' INTO TABLE MIS_PAR_REL_EXP_TYP;

--- Llenado de Tablas de Dimensiones Adicionales ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_REG_DIMENSIONS; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_REG_DIMENSIONS.csv' INTO TABLE MIS_PAR_REL_REG_DIMENSIONS;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_MUL_LAT; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_MUL_LAT.csv' INTO TABLE MIS_PAR_CAT_MUL_LAT;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ECO_GRO; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_ECO_GRO.csv' INTO TABLE MIS_PAR_CAT_ECO_GRO;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CONV; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_CONV.csv' INTO TABLE MIS_PAR_CAT_CONV;

--- Llenado de Tablas de Catálogo de Fuente de Comisiones ---
TRUNCATE TABLE IF EXISTS MIS_PAR_COM_GL; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_COM_GL.csv' INTO TABLE MIS_PAR_COM_GL;

--- Llenado de Tablas Catálogo Producto / Subproducto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PRODUCT; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_PRODUCT.csv' INTO TABLE MIS_PAR_CAT_PRODUCT;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PROG_CARD; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_PROG_CARD.csv' INTO TABLE MIS_PAR_CAT_PROG_CARD;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_SUBPRODUCT; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_SUBPRODUCT.csv' INTO TABLE MIS_PAR_CAT_SUBPRODUCT;

--- Llenado de Tablas de Remarcaje de Producto por Contrato ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PROD_SPE; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_PROD_SPE.csv' INTO TABLE MIS_PAR_REL_PROD_SPE;

--- Llenado de Tablas de Grupo Economico Regional y Sector Economico Regional---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ECO_GRO_REG; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CAT_ECO_GRO_REG.csv' INTO TABLE MIS_PAR_CAT_ECO_GRO_REG;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_ECO_GRO_REG; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_REL_ECO_GRO_REG.csv' INTO TABLE MIS_PAR_REL_ECO_GRO_REG;

TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PROD_BL; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_HIERARCHY_PROD_BL.csv' INTO TABLE MIS_HIERARCHY_PROD_BL;

--- Llenado de la Tabla de Catálogo de Sucursales ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CNTRLBRN; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/CNTRLBRN.csv' INTO TABLE MIS_LOAD_CNTRLBRN;

TRUNCATE TABLE MIS_PAR_CAT_OFFI;
INSERT INTO MIS_PAR_CAT_OFFI
SELECT DISTINCT BRNBRN, BRNDSC FROM MIS_LOAD_CNTRLBRN WHERE BRNBNK = '01'
UNION ALL
SELECT * FROM MIS_PAR_CAT_OFFI_MANUAL
;

--- Llenado de la Tabla de Catálogo de Centros de Costo ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CCDSC; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/CCDSC.csv' INTO TABLE MIS_LOAD_CCDSC;

INSERT OVERWRITE MIS_PAR_CAT_ACCO_CENT
SELECT a.CCDBNK, a.CCDCCN, a.CCDDSC
FROM MIS_LOAD_CCDSC a 
WHERE a.CCDDAT = '${var:periodo}'
AND a.CCDBNK = '01'
;

--- Llenado de la Tabla de Catálogo de Oficiales ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CNOFC; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/CNOFC.csv' INTO TABLE MIS_LOAD_CNOFC;

--- Llenado de la Tabla de Tasas de Cambio ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_RATES; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/RATES.csv' INTO TABLE MIS_LOAD_RATES;

ALTER TABLE MIS_PAR_EXCH_RATE
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_PAR_EXCH_RATE 
PARTITION (DATA_DATE, COD_CONT)
SELECT a.RATCCY, a.RATEXR, NULL AS COD_ENTITY, a.RATDAT, 'TC_FOTO' AS COD_CONT
FROM MIS_LOAD_RATES a 
WHERE a.RATDAT = '${var:periodo}'
AND a.RATBNK = '01'
;

--- Llenado de Tabla de Curvas ---
TRUNCATE TABLE MIS_LOAD_CURVES;

ALTER TABLE MIS_LOAD_CURVES
ADD IF NOT EXISTS PARTITION (DATA_DATE = '${var:periodo}');

LOAD DATA INPATH '${var:ruta_fuentes_activos}/MIS_PAR_CURVES.csv' INTO TABLE MIS_LOAD_CURVES PARTITION (DATA_DATE = '${var:periodo}');


--------------- AJUSTE DE TTI ESPECIAL --------------- 

ALTER TABLE MIS_PAR_TTI_SPE
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_PAR_TTI_SPE
PARTITION (DATA_DATE = '${var:periodo}')
SELECT *
FROM MIS_LOAD_TTI_SPE a
;


------------------- AJUSTE DE CURVAS ------------------- 

ALTER TABLE MIS_PAR_CURVES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Interpolar: calcular la tasa de los puntos intermedios no informados en la curva parametrizada del día
INSERT INTO MIS_PAR_CURVES (COD_CURVE, COD_CURRENCY, TERM, TERM_UNIT, RATE)
PARTITION (DATA_DATE)  
WITH 
STEP_1 AS ( --Agregar a tabla de datos informada el siguiente valor (LEAD) para definir un rango
    SELECT a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.RATE, 
        LEAD(a.TERM, 1) OVER (PARTITION BY a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM_UNIT ORDER BY a.TERM) AS NEXT_TERM, 
        LEAD(a.RATE, 1) OVER (PARTITION BY a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM_UNIT ORDER BY a.TERM) AS NEXT_RATE
    FROM MIS_LOAD_CURVES a
    ),
STEP_2 AS ( --Obtener los puntos a los cuales se les asignará una tasa por medio de interporlación
    SELECT DISTINCT a.NUMBER AS TERM
    FROM MIS_NUMBERS a     
    WHERE a.NUMBER > 0 AND a.NUMBER <= (SELECT MAX(TERM) FROM STEP_1)),
STEP_3 AS ( --Agregar a cada punto el rango que deberá usar para interpolar
    SELECT b.COD_CURVE, b.DATA_DATE, b.COD_CURRENCY, a.TERM, b.TERM_UNIT, b.TERM AS PREV_TERM, b.RATE AS PREV_RATE, b.NEXT_TERM, b.NEXT_RATE
    FROM STEP_2 a 
    INNER JOIN STEP_1 b 
    ON (a.TERM > b.TERM AND a.TERM < b.NEXT_TERM) OR (a.TERM = b.TERM)),
STEP_4 AS (--Agregar el número de fila (ROW_NUMBER) o distancia entre el punto y su valor minimo dentro del rango que se encuentra
    SELECT a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.PREV_TERM, a.PREV_RATE, a.NEXT_TERM, a.NEXT_RATE, 
        ROW_NUMBER() OVER (PARTITION BY a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM_UNIT, a.PREV_TERM ORDER BY a.TERM) AS RN
    FROM STEP_3 a)
--Interpolar usando los valores previamente calculados, solo se interpola cuando es un punto intermedio y no fue informado 
SELECT DISTINCT a.COD_CURVE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT,  
    CAST(CASE WHEN a.TERM=a.PREV_TERM THEN a.PREV_RATE
         ELSE a.PREV_RATE + (((a.NEXT_RATE - a.PREV_RATE)/(a.NEXT_TERM - a.PREV_TERM)) * (a.RN - 1)) 
         END AS decimal(30, 10)) AS RATE, a.DATA_DATE
FROM STEP_4 a;


---- Replicar curva más reciente en caso de que no haya sido parametrizada para algún día (Ej: fin de semana, festivo)
INSERT INTO MIS_PAR_CURVES (COD_CURVE, COD_CURRENCY, TERM, TERM_UNIT, RATE)
PARTITION (DATA_DATE)
WITH 
DATES_INF AS ( --Identificación de curvas y fechas cargadas
    SELECT a.COD_CURVE, TO_TIMESTAMP(a.DATA_DATE, 'yyyyMMdd') AS DATA_DATE, a.COD_CURRENCY, a.TERM_UNIT
    FROM MIS_PAR_CURVES a
    GROUP BY a.COD_CURVE, a.DATA_DATE, a.COD_CURRENCY, a.TERM_UNIT),
DATES_REQ AS ( --Calculo de días entre las curvas cargadas
    SELECT a.COD_CURVE, a.DATA_DATE AS PREV_DATE, a.COD_CURRENCY, a.TERM_UNIT,
        LEAD(a.DATA_DATE, 1, DATE_ADD(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1)) OVER (PARTITION BY a.COD_CURVE, a.COD_CURRENCY, a.TERM_UNIT ORDER BY a.DATA_DATE) AS NEXT_DATE,
        DATEDIFF(LEAD(a.DATA_DATE, 1, DATE_ADD(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1)) OVER (PARTITION BY a.COD_CURVE, a.COD_CURRENCY, a.TERM_UNIT ORDER BY a.DATA_DATE), a.DATA_DATE) AS INTER_DAYS 
    FROM DATES_INF a),
DATE_AUX AS ( --Número de días a sumar
    SELECT DISTINCT a.NUMBER AS DAYS
    FROM MIS_NUMBERS a
    WHERE a.NUMBER > 0)
--Creación de curvas que no fueron cargadas entre dos fechas cargadas
SELECT DISTINCT a.COD_CURVE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.RATE, 
    FROM_TIMESTAMP(DATE_ADD(b.PREV_DATE, c.DAYS), 'yyyyMMdd') AS DATA_DATE
FROM MIS_PAR_CURVES a 
INNER JOIN (SELECT * FROM DATES_REQ WHERE INTER_DAYS > 1 AND INTER_DAYS < 8 ) b  --Filtro para controlar el número de días que se puede replicar 
ON a.COD_CURVE = b.COD_CURVE AND a.COD_CURRENCY = b.COD_CURRENCY AND a.TERM_UNIT = b.TERM_UNIT AND a.DATA_DATE = FROM_TIMESTAMP(b.PREV_DATE, 'yyyyMMdd')
INNER JOIN DATE_AUX c 
ON DATE_ADD(b.PREV_DATE, c.DAYS) < b.NEXT_DATE;


---- Crear curvas antiguas no existentes en caso de que media móvil lo requiera
INSERT INTO MIS_PAR_CURVES (COD_CURVE, COD_CURRENCY, TERM, TERM_UNIT, RATE)
PARTITION (DATA_DATE)
WITH 
DATE_REQ AS ( --Identificar la fecha más antigua requerida para el método TTI Media móvil
    SELECT COD_CURVE, COD_CURRENCY, TERM_UNIT, DATE_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), MAX(AVG_PERIOD)-1) AS FIRST_DATE_REQ
    FROM MIS_PAR_TTI_ENG
    WHERE METHOD_TTI = 'MA'
    GROUP BY COD_CURVE, COD_CURRENCY, TERM_UNIT),
DATE_INF AS ( --Identificar fecha más antigua informada para cada curva
    SELECT COD_CURVE, COD_CURRENCY, TERM_UNIT, MIN(DATA_DATE) AS FIRST_DATE_INFO
    FROM MIS_PAR_CURVES 
    WHERE TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') <= TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') --Garantiza que la fecha de ejecución sea mayor que la fecha de la curva a replicar
    GROUP BY COD_CURVE, COD_CURRENCY, TERM_UNIT),
DATE_AUX AS ( --Número de días a restar
    SELECT DISTINCT a.NUMBER AS DAYS
    FROM MIS_NUMBERS a
    WHERE a.NUMBER > 0)
--Replicar la curva más antigua informada hasta la fecha más antigua requerida por el método
SELECT DISTINCT a.COD_CURVE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.RATE, 
    FROM_TIMESTAMP(DATE_SUB(TO_TIMESTAMP(b.FIRST_DATE_INFO, 'yyyyMMdd'), d.DAYS), 'yyyyMMdd') AS DATA_DATE
FROM MIS_PAR_CURVES a 
INNER JOIN DATE_INF b 
ON a.COD_CURVE = b.COD_CURVE AND a.COD_CURRENCY = b.COD_CURRENCY AND a.TERM_UNIT = b.TERM_UNIT AND a.DATA_DATE = b.FIRST_DATE_INFO
INNER JOIN DATE_REQ c 
ON b.COD_CURVE = c.COD_CURVE AND b.COD_CURRENCY = c.COD_CURRENCY AND b.TERM_UNIT = c.TERM_UNIT
INNER JOIN DATE_AUX d 
ON DATE_SUB(TO_TIMESTAMP(b.FIRST_DATE_INFO, 'yyyyMMdd'), d.DAYS) >= c.FIRST_DATE_REQ;