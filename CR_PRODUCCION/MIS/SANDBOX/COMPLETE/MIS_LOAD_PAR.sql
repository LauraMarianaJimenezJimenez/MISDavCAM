------------------------------------------------ CREACIÓN TABLAS DE PARÁMETROS DE RELACIONES ------------------------------------------------ 

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de tabla de jerarquía de Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_PL_ACC.csv' INTO TABLE MIS_HIERARCHY_PL_ACC;

--- Llenado de tabla de jerarquía de Producto de Balance ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BLCE_PROD; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_BLCE_PROD.csv' INTO TABLE MIS_HIERARCHY_BLCE_PROD;

--- Llenado de tabla de Entidades ---
TRUNCATE TABLE IF EXISTS MIS_PAR_ENTITY; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ENTITY.csv' INTO TABLE MIS_PAR_ENTITY;

--- Llenado de tabla de jerarquía de Cuenta P&G Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PL_ACC_LC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_PL_ACC_LC.csv' INTO TABLE MIS_HIERARCHY_PL_ACC_LC;

--- Llenado de tabla de jerarquía de Producto de Balance Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BLCE_PROD_LC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_BLCE_PROD_LC.csv' INTO TABLE MIS_HIERARCHY_BLCE_PROD_LC;

--- Llenado de tabla de jerarquía de Producto de Balance Local ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_BL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_BL.csv' INTO TABLE MIS_HIERARCHY_BL;

--- Llenado de tabla de jerarquía de Unidad de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_UN; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_UN.csv' INTO TABLE MIS_HIERARCHY_UN;


--- Llenado de la tabla de Concepto Contable y Código de Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CAF;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_CAF.csv' INTO TABLE MIS_PAR_CAT_CAF;

--- Llenado de la Tabla de Catálogo de Cuenta de Gestión ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PL;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_PL.csv' INTO TABLE MIS_PAR_CAT_PL;

--- Llenado de la Tabla de Catálogo de Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BP;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_BP.csv' INTO TABLE MIS_PAR_CAT_BP;

--- Llenado de la Tabla de Catálogo de Líneas de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_BL;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_BL.csv' INTO TABLE MIS_PAR_CAT_BL;

--- Llenado de la Tabla de Parametría Cuenta Contable/Agrupador Contable ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_CAF_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_CAF_ACC.csv' INTO TABLE MIS_PAR_REL_CAF_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Cuenta P&G ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_PL_ACC.csv' INTO TABLE MIS_PAR_REL_PL_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable/Producto Balance ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BP_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_BP_ACC.csv' INTO TABLE MIS_PAR_REL_BP_ACC;

--- Llenado de la Tabla de Parametría Producto Balance/Línea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BL_ACC; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_BL_ACC.csv' INTO TABLE MIS_PAR_REL_BL_ACC;

--- Llenado de la Tabla de Parametría Agrupador Contable Operacional ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_CAF_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_CAF_OPER.csv' INTO TABLE MIS_PAR_REL_CAF_OPER;

--- Llenado de la Tabla de Parametría Producto Balance Operacional ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BP_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_BP_OPER.csv' INTO TABLE MIS_PAR_REL_BP_OPER;

--- Llenado de la Tabla de Parametría Línea de Negocio Operacional ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_BL_OPER; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_BL_OPER.csv' INTO TABLE MIS_PAR_REL_BL_OPER;

--- Llenado de la Tabla de Parametría de Dimensiones Regionales ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_REG_DIMENSIONS; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_REG_DIMENSIONS.csv' INTO TABLE MIS_PAR_REL_REG_DIMENSIONS;

--- Llenado de tabla de Catalogo de Convenios ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_CONV; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_CONV.csv' INTO TABLE MIS_PAR_CAT_CONV;

--- Llenado de tabla de Catalogo de Grupo Económico  ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ECO_GRO; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_ECO_GRO.csv' INTO TABLE MIS_PAR_CAT_ECO_GRO;

--- Llenado de tabla de catalogo Multilatino ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_MUL_LAT; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_MUL_LAT.csv' INTO TABLE MIS_PAR_CAT_MUL_LAT;


--- Llenado de la Tabla de Parametría Reparto de Gastos por Centro de Costo ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_AC_ENG.csv' INTO TABLE MIS_PAR_ALLOC_AC_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto de Gastos por Centro de Costo ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_AC_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_AC_DRI.csv' INTO TABLE MIS_PAR_ALLOC_AC_DRI;

--- Llenado de la Tabla de Parametría Reparto de Gastos por Segmento/Producto ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_SEG_ENG.csv' INTO TABLE MIS_PAR_ALLOC_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto de Gastos por Segmento/Producto ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_ALLOC_SEG_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_ALLOC_SEG_DRI.csv' INTO TABLE MIS_PAR_ALLOC_SEG_DRI;

--- Llenado de la Tabla de Parametría Familia de Centros de Costo ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_EXP_TYP; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_EXP_TYP.csv' INTO TABLE MIS_PAR_REL_EXP_TYP;

--- Llenado de la Tabla de Parametría Reparto Internegocios/Comisiones por Segmento/Producto ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_INTER_SEG_ENG.csv' INTO TABLE MIS_PAR_INTER_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto Internegocios/Comisiones por Segmento/Producto ---
--TRUNCATE TABLE IF EXISTS MIS_PAR_INTER_SEG_DRI; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_INTER_SEG_DRI.csv' INTO TABLE MIS_PAR_INTER_SEG_DRI;

--- Llenado de la Tabla de Parametría Reparto del balance por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_BALAN_SEG_ENG; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_BALAN_SEG_ENG.csv' INTO TABLE MIS_PAR_BALAN_SEG_ENG;

--- Llenado de la Tabla de Parametría Drivers de Reparto del Balance por Segmento ---
TRUNCATE TABLE IF EXISTS MIS_PAR_BALAN_SEG_DRI; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_BALAN_SEG_DRI.csv' INTO TABLE MIS_PAR_BALAN_SEG_DRI;

--- Llenado de la Tabla Jerarquia Producto por Linea de Negocio ---
TRUNCATE TABLE IF EXISTS MIS_HIERARCHY_PROD_BL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_HIERARCHY_PROD_BL.csv' INTO TABLE MIS_HIERARCHY_PROD_BL;

--- Llenado de la Tabla de Catálogo de Sucursales ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_OFFI;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_OFFI.csv' INTO TABLE MIS_PAR_CAT_OFFI;

--- Llenado de la Tabla de Catálogo de Centro de costos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ACCO_CENT;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_ACCO_CENT.csv' INTO TABLE MIS_PAR_CAT_ACCO_CENT;

--- Llenado de la Tabla de Catálogo de Código Gastos ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_EXPENSE;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_EXPENSE.csv' INTO TABLE MIS_PAR_CAT_EXPENSE;

--- Llenado de la Tabla de Catálogo clientes institucionales  ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_INST_CLI;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_INST_CLI.csv' INTO TABLE MIS_PAR_CAT_INST_CLI;

--- Llenado de la Tablas Grupo Economico  ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_ECO_GRO_REG;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_ECO_GRO_REG.csv' INTO TABLE MIS_PAR_CAT_ECO_GRO_REG;

TRUNCATE TABLE IF EXISTS MIS_PAR_REL_ECO_GRO_REG;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_ECO_GRO_REG.csv' INTO TABLE MIS_PAR_REL_ECO_GRO_REG;

--- Llenado de la Tabla de Parametría Programa de lealtad ---
TRUNCATE TABLE IF EXISTS MIS_PAR_REL_PROG_CARD;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_REL_PROG_CARD.csv' INTO TABLE MIS_PAR_REL_PROG_CARD;

--- Llenado de la Tabla de Parametría Sector Economico ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_SECTOR_ECO;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_SECTOR_ECO.csv' INTO TABLE MIS_PAR_CAT_SECTOR_ECO;

--- Llenado de la Tabla de Parametría Manager ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_MANAGER;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_MANAGER.csv' INTO TABLE MIS_PAR_CAT_MANAGER;

--- Llenado de la Tabla de Parametría Producto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_PRODUCT;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_PRODUCT.csv' INTO TABLE MIS_PAR_CAT_PRODUCT;

--- Llenado de la Tabla de Parametría Subproducto ---
TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_SUBPRODUCT;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CAT_SUBPRODUCT.csv' INTO TABLE MIS_PAR_CAT_SUBPRODUCT;

--STOP ESPERADO

--- Llenado de la Tabla Tipo/tasa de cambio ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_EXCH_RATE; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/EXCH_RATE.csv' INTO TABLE MIS_LOAD_EXCH_RATE;

--- Inserción de tasa de cambio en tabla ---
INSERT OVERWRITE MIS_PAR_EXCH_RATE 
PARTITION (DATA_DATE, COD_CONT)
SELECT a.COD_CURRENCY, a.EXCH_RATE, NULL AS COD_ENTITY, from_timestamp(to_timestamp(a.DATA_DATE,'dd/MM/yyyy'),'yyyyMMdd') AS DATA_DATE, a.COD_CONT
FROM MIS_LOAD_EXCH_RATE a 
WHERE '${var:periodo}' = from_timestamp(to_timestamp(a.DATA_DATE,'dd/MM/yyyy'),'yyyyMMdd') OR STRLEFT(a.COD_CONT, 4)='PPTO';

INSERT OVERWRITE MIS_PAR_EXCH_RATE 
PARTITION (DATA_DATE, COD_CONT)
SELECT a.COD_CURRENCY, CAST(AVG(a.EXCH_RATE) AS decimal(30, 10)) AS EXCH_RATE, NULL AS COD_ENTITY, '${var:periodo}' AS DATA_DATE, 'TC_PROMEDIO' AS COD_CONT
FROM MIS_PAR_EXCH_RATE a 
WHERE a.COD_CONT='TC_FOTO' AND 
TO_TIMESTAMP(a.DATA_DATE, 'yyyyMMdd') BETWEEN TRUNC(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'),'YY') AND TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')
GROUP BY a.COD_CURRENCY;


--- Llenado de la Tabla de Parametría del Motor de Impuestos---
--TRUNCATE TABLE IF EXISTS MIS_PAR_TAX_ENG; 
--LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_TAX_ENG.csv' INTO TABLE MIS_PAR_TAX_ENG;

--- Llenado de Tabla de Parametría de Motor de TTI ---
TRUNCATE TABLE MIS_PAR_TTI_ENG;
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_TTI_ENG.csv' INTO TABLE MIS_PAR_TTI_ENG;

--- Llenado de la Tabla de Tasas especiales por contrato ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_TTI_SPE; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_TTI_SPE.csv' INTO TABLE MIS_LOAD_TTI_SPE;

--- Llenado de la Tabla de Tasas especiales por contrato ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_PAR_CURVES; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/MIS_PAR_CURVES.csv' INTO TABLE MIS_LOAD_PAR_CURVES;


--------------- AJUSTE DE TTI ESPECIAL --------------- 

ALTER TABLE MIS_PAR_TTI_SPE
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

----Inserción de contratos iguales a la fecha de ejecución
INSERT INTO MIS_PAR_TTI_SPE
PARTITION (DATA_DATE = '${var:periodo}')
SELECT IDF_CTO, COD_VALUE,
   CAST(a.RATE_TTI/100 AS decimal(30, 10)) AS RATE_TTI,
   CAST(a.RATE_LIQ/100 AS decimal(30, 10)) AS RATE_LIQ,
   CAST(a.RATE_ENC/100 AS decimal(30, 10)) AS RATE_ENC,
   CAST(a.RATE_PEA/100 AS decimal(30, 10)) AS RATE_PEA,
   CAST(a.RATE_POR/100 AS decimal(30, 10)) AS RATE_POR
FROM MIS_LOAD_TTI_SPE a
;

--- Llenado de la Tabla de Curvas de Captación en colones ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_CAP_COL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_CAPTACION_COL.csv' INTO TABLE MIS_LOAD_CUR_CAP_COL;

--- Llenado de la Tabla de Curvas de Captación en dólares ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_CAP_DOL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_CAPTACION_DOL.csv' INTO TABLE MIS_LOAD_CUR_CAP_DOL;

--- Llenado de la Tabla de Curvas de Colocación en colones ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_CLC_COL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_COLOCACION_COL.csv' INTO TABLE MIS_LOAD_CUR_CLC_COL;

--- Llenado de la Tabla de Curvas de Colocación en dólares ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_CLC_DOL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_COLOCACION_DOL.csv' INTO TABLE MIS_LOAD_CUR_CLC_DOL;

--- Llenado de la Tabla de Curvas de Pipca en colones ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_PIP_COL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_PIPCA_COL.csv' INTO TABLE MIS_LOAD_CUR_PIP_COL;

--- Llenado de la Tabla de Curvas de Pipca en dólares ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_CUR_PIP_DOL; 
LOAD DATA INPATH '${var:ruta_fuentes_completo}/CURVA_PIPCA_DOL.csv' INTO TABLE MIS_LOAD_CUR_PIP_DOL;

--- Limpieza de partición ----
ALTER TABLE MIS_LOAD_CURVES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Inserción de curvas manuales en tabla auxiliar ---
INSERT INTO MIS_LOAD_CURVES 
PARTITION (DATA_DATE)
SELECT a.COD_CURVE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.RATE, a.DATA_DATE
FROM MIS_LOAD_PAR_CURVES a
WHERE TRIM(a.DATA_DATE) = '${var:periodo}';

--- Inserción de curvas en tabla auxiliar ---
INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'CAPTACION_CRC' AS COD_CURVE, 'CRC' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.CURVA
FROM MIS_LOAD_CUR_CAP_COL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'CAPTACION_USD' AS COD_CURVE, 'USD' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.CURVA
FROM MIS_LOAD_CUR_CAP_DOL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'COLOCACION_CRC' AS COD_CURVE, 'CRC' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.CURVA
FROM MIS_LOAD_CUR_CLC_COL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'COLOCACION_USD' AS COD_CURVE, 'USD' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.CURVA
FROM MIS_LOAD_CUR_CLC_DOL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'COLOCACION_CRC_PL' AS COD_CURVE, 'CRC' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.PRIMA_LIQ
FROM MIS_LOAD_CUR_CLC_COL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'COLOCACION_USD_PL' AS COD_CURVE, 'USD' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.PRIMA_LIQ
FROM MIS_LOAD_CUR_CLC_DOL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'CURVA_PIPCA_CRC' AS COD_CURVE, 'CRC' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CUR.CURVA
FROM MIS_LOAD_CUR_PIP_COL CUR; 

INSERT INTO MIS_LOAD_CURVES
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'CURVA_PIPCA_USD' AS COD_CURVE, 'USD' AS COD_CURRENCY, CUR.DIAS, 'D' AS TERM_UNIT, CAST(REGEXP_REPLACE(CUR.CURVA, ',', '.') AS decimal(30, 10))
FROM MIS_LOAD_CUR_PIP_DOL CUR; 

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
    WHERE a.DATA_DATE = '${var:periodo}' ),
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
/*INSERT INTO MIS_PAR_CURVES (COD_CURVE, COD_CURRENCY, TERM, TERM_UNIT, RATE)
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
ON DATE_ADD(b.PREV_DATE, c.DAYS) < b.NEXT_DATE;*/


---- Crear curvas antiguas no existentes en caso de que media móvil lo requiera
/*INSERT INTO MIS_PAR_CURVES (COD_CURVE, COD_CURRENCY, TERM, TERM_UNIT, RATE)
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
    WHERE a.NUMBER BETWEEN 1 AND (SELECT MAX(AVG_PERIOD) FROM MIS_PAR_TTI_ENG))
--Replicar la curva más antigua informada hasta la fecha más antigua requerida por el método
SELECT DISTINCT a.COD_CURVE, a.COD_CURRENCY, a.TERM, a.TERM_UNIT, a.RATE, 
    FROM_TIMESTAMP(DATE_SUB(TO_TIMESTAMP(b.FIRST_DATE_INFO, 'yyyyMMdd'), d.DAYS), 'yyyyMMdd') AS DATA_DATE
FROM MIS_PAR_CURVES a 
INNER JOIN DATE_INF b 
ON a.COD_CURVE = b.COD_CURVE AND a.COD_CURRENCY = b.COD_CURRENCY AND a.TERM_UNIT = b.TERM_UNIT AND a.DATA_DATE = b.FIRST_DATE_INFO
INNER JOIN DATE_REQ c 
ON b.COD_CURVE = c.COD_CURVE AND b.COD_CURRENCY = c.COD_CURRENCY AND b.TERM_UNIT = c.TERM_UNIT
INNER JOIN DATE_AUX d 
ON DATE_SUB(TO_TIMESTAMP(b.FIRST_DATE_INFO, 'yyyyMMdd'), d.DAYS) >= c.FIRST_DATE_REQ;*/