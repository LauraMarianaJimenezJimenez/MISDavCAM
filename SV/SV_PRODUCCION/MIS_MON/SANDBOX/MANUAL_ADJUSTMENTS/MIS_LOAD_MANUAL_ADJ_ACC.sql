--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

ALTER TABLE MIS_DWH_MANUAL_ADJ_ACC
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

ALTER TABLE MIS_DWH_MANUAL_ADJ_ACC
ADD IF NOT EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--- Llenado de Tabla de Ajustes Manuales Contables ---
LOAD DATA INPATH '${var:ruta_fuentes_ajustes}/Ajustes_Manuales_Contables.csv' INTO TABLE MIS_DWH_MANUAL_ADJ_ACC PARTITION (DATA_DATE = '${var:periodo}');

--- Limpieza de tabla de rechazos por ajustes manuales contables ---
TRUNCATE TABLE MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS;

---Rechazo de registros ingresados sin alguna de las dimensiones principales
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'Rechazo: existen dimensiones vacías', '', a.AGRUPADOR_CONTABLE, a.ENTIDAD, a.DIVISA, '', '', a.PRODUCTO_BALANCE, a.CUENTA_PYG, '', '', '', 
    a.IMPORTE_ML, a.IMPORTE_MO, a.SALDO_MEDIO_ML, a.SALDO_MEDIO_MO, a.PL_ML, a.PL_MO
FROM MIS_DWH_MANUAL_ADJ_ACC a
WHERE TRIM(a.AGRUPADOR_CONTABLE) = '' OR TRIM(a.CUENTA_PYG) = '' OR TRIM(a.PRODUCTO_BALANCE) = '' OR TRIM(a.LINEA_NEGOCIO) = '' 
    OR TRIM(a.ENTIDAD) = '' OR TRIM(a.DIVISA) = '';

---Validación de cierre: Agrupador Contable no existe o es diferente del definido en la parametria
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'Validación: AGRUPADOR CONTABLE no encontrado en tablas de parametria', '', 
    a.AGRUPADOR_CONTABLE, a.ENTIDAD, a.DIVISA, '', '', a.PRODUCTO_BALANCE, a.CUENTA_PYG, '', '', '', 
    a.IMPORTE_ML, a.IMPORTE_MO, a.SALDO_MEDIO_ML, a.SALDO_MEDIO_MO, a.PL_ML, a.PL_MO
FROM MIS_DWH_MANUAL_ADJ_ACC a
LEFT JOIN MIS_PAR_REL_CAF_ACC b
ON b.COD_ENTITY = a.ENTIDAD AND b.COD_CURRENCY = a.DIVISA AND b.COD_GL_GROUP = a.AGRUPADOR_CONTABLE
WHERE b.COD_GL_GROUP IS NULL;

---Validación de cierre: Cuenta PyG no existe o es diferente del definido en la parametria
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'Validación: CUENTA DE GESTION no encontrada en tablas de parametria', '', 
    a.AGRUPADOR_CONTABLE, a.ENTIDAD, a.DIVISA, '', '', a.PRODUCTO_BALANCE, a.CUENTA_PYG, '', '', '', 
    a.IMPORTE_ML, a.IMPORTE_MO, a.SALDO_MEDIO_ML, a.SALDO_MEDIO_MO, a.PL_ML, a.PL_MO
FROM MIS_DWH_MANUAL_ADJ_ACC a
LEFT JOIN MIS_PAR_REL_PL_ACC b
ON b.COD_ENTITY = a.ENTIDAD AND b.COD_CURRENCY = a.DIVISA AND b.COD_GL_GROUP = a.AGRUPADOR_CONTABLE 
WHERE b.COD_PL_ACC IS NULL;

---Validación de cierre: Producto de Balance no existe o es diferente del definido en la parametria
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'Validación: PRODUCTO DE BALANCE no encontrado en tablas de parametria', '', 
    a.AGRUPADOR_CONTABLE, a.ENTIDAD, a.DIVISA, '', '', a.PRODUCTO_BALANCE, a.CUENTA_PYG, '', '', '', 
    a.IMPORTE_ML, a.IMPORTE_MO, a.SALDO_MEDIO_ML, a.SALDO_MEDIO_MO, a.PL_ML, a.PL_MO
FROM MIS_DWH_MANUAL_ADJ_ACC a
LEFT JOIN MIS_PAR_REL_BP_ACC b
ON b.COD_ENTITY = a.ENTIDAD AND b.COD_CURRENCY = a.DIVISA AND b.COD_GL_GROUP = a.AGRUPADOR_CONTABLE 
WHERE b.COD_BLCE_PROD IS NULL;

---Validación de cierre: Línea de Negocio no existe o es diferente del definido en la parametria
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'Validación: LINEA DE NEGOCIO no encontrada en tablas de parametria', '', 
    a.AGRUPADOR_CONTABLE, a.ENTIDAD, a.DIVISA, '', '', a.PRODUCTO_BALANCE, a.CUENTA_PYG, '', '', '', 
    a.IMPORTE_ML, a.IMPORTE_MO, a.SALDO_MEDIO_ML, a.SALDO_MEDIO_MO, a.PL_ML, a.PL_MO
FROM MIS_DWH_MANUAL_ADJ_ACC a
LEFT JOIN MIS_PAR_REL_BL_ACC b
ON b.COD_ENTITY = a.ENTIDAD AND b.COD_GL_GROUP = a.AGRUPADOR_CONTABLE 
WHERE b.COD_BUSINESS_LINE IS NULL;


---Registros que cumplen con la validación (La llave Entidad - Agrupador contable - Divisa, existe)
INSERT INTO MIS_DM_BALANCE_RESULT_M
(COD_CONT,COD_CURRENCY,COD_ENTITY,IDF_CTO,COD_GL,DES_GL,COD_ACCO_CENT,COD_OFFI,COD_NAR,COD_BLCE_STATUS,COD_PRODUCT,COD_SUBPRODUCT,
EOPBAL_TOT,EOPBAL_CAP,EOPBAL_INT,AVGBAL_TOT,AVGBAL_CAP,AVGBAL_INT,PL,PL_TOT,
COD_TYP_FEE,IDF_CLI,TYP_DOC_ID,NUM_DOC_ID,NOM_NAME,NOM_LASTN,COD_RES_COUNT,COD_TYP_CLNT,COD_COND_CLNT,COD_ENG,VAL_SEX,COD_SEGMENT,DES_SEGMENT,
IND_EMPL_GRUP,DATE_CLNT_REG,DATE_CLNT_WITHDRAW,COD_MANAGER,COD_SECTOR,DES_SECTOR,RATING_CLI,COD_BCA_INT,COD_AMRT_MET,COD_RATE_TYPE,
RATE_INT,DATE_ORIGIN,DATE_LAST_REV,DATE_PRX_REV,EXP_DATE,FREQ_INT_PAY,COD_UNI_FREQ_INT_PAY,FRE_REV_INT,COD_UNI_FRE_REV_INT,
AMRT_TRM,COD_UNI_AMRT_TRM,INI_AM,CUO_AM,CREDIT_LIM_AM,PREDEF_RATE_IND,PREDEF_RATE,IND_CHANNEL,
EOPBAL_TOT_RPT,AVGBAL_TOT_RPT,PL_RPT,PL_TOT_RPT,
EOPBAL_TOT_USD,EOPBAL_TOT_REG,AVGBAL_TOT_USD,AVGBAL_TOT_REG,PL_TOT_USD,PL_TOT_REG,
FTP_RESULT_RPT,
COD_GL_GROUP,DES_GL_GROUP,ACCOUNT_CONCEPT,COD_PL_ACC,DES_PL_ACC,COD_BLCE_PROD,DES_BLCE_PROD,COD_BUSINESS_LINE,DES_BUSINESS_LINE,FTP,FTP_RESULT,IND_POOL,COD_SUBSEGMENT,COD_GROUP_EC)
PARTITION (DATA_DATE,COD_INFO_SOURCE)
SELECT 'RCTB', a.DIVISA, a.ENTIDAD, '', '', '', ISNULL(a.CENTRO_COSTO,''), '', '', '',  '', '', 
    CAST(IFNULL(a.IMPORTE_MO, a.IMPORTE_ML) AS DECIMAL(30,10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.IMPORTE_MO, a.IMPORTE_ML) AS DECIMAL(30,10)) AS EOPBAL_CAP, 
    0 AS EOPBAL_INT, 
    CAST(IFNULL(a.SALDO_MEDIO_MO, a.IMPORTE_ML) AS DECIMAL(30,10)) AS AVGBAL_TOT,  
    CAST(IFNULL(a.SALDO_MEDIO_MO, a.IMPORTE_ML) AS DECIMAL(30,10)) AS AVGBAL_CAP, 
    0 AS AVGBAL_INT, 
    CAST(IFNULL(a.PL_MO, a.PL_ML) AS DECIMAL(30,10)) AS PL, 
    CAST(IFNULL(a.PL_MO, a.PL_ML) AS DECIMAL(30,10)) AS PL_TOT, 
    '', '', '', '', '', '', '', '', '', '', '', '', '', 
    '', '', '', '', '', '', '', '', '', '', 
    NULL, '', '', '', '',  NULL, '', NULL, '', 
    NULL, '', NULL, NULL, NULL, '', NULL, '', 
    CAST(a.IMPORTE_ML AS DECIMAL(30,10)) AS EOPBAL_TOT_RPT, 
    CAST(a.SALDO_MEDIO_ML AS DECIMAL(30,10)) AS AVGBAL_TOT_RPT, 
    CAST(a.PL_ML AS DECIMAL(30,10)) AS PL_RPT, 
    CAST(a.PL_ML AS DECIMAL(30,10)) AS PL_TOT_RPT, 
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.IMPORTE_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.IMPORTE_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS EOPBAL_TOT_USD,
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.IMPORTE_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.IMPORTE_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS EOPBAL_TOT_REG,
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.SALDO_MEDIO_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.SALDO_MEDIO_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS AVGBAL_TOT_USD,
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.SALDO_MEDIO_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.SALDO_MEDIO_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS AVGBAL_TOT_REG,
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.PL_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.PL_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS PL_TOT_USD,
    CAST(CASE WHEN a.DIVISA='COP' THEN IFNULL(a.PL_ML / j.EXCH_RATE, 0) 
        ELSE IFNULL(a.PL_ML * j.EXCH_RATE, 0) END AS decimal(30, 10)) AS PL_TOT_REG,
    NULL, 
    a.AGRUPADOR_CONTABLE,  c.DES_GL_GROUP, c.ACCOUNT_CONCEPT, a.CUENTA_PYG, g.DES_PL_ACC, a.PRODUCTO_BALANCE, e.DES_BLCE_PROD, a.LINEA_NEGOCIO, i.DES_BUSINESS_LINE, 
    NULL, NULL, '', '', '', '${var:periodo}', CONCAT('AJUSTE_',a.CRITERIO_DE_AJUSTE) AS COD_INFO_SOURCE
FROM MIS_DWH_MANUAL_ADJ_ACC a 
LEFT JOIN MIS_PAR_REL_CAF_ACC b
ON b.COD_ENTITY = a.ENTIDAD AND b.COD_GL = a.AGRUPADOR_CONTABLE AND b.COD_CURRENCY = a.DIVISA
LEFT JOIN MIS_PAR_CAT_CAF AS c
ON a.AGRUPADOR_CONTABLE = c.COD_GL_GROUP AND a.DIVISA=c.COD_CURRENCY
LEFT JOIN MIS_PAR_REL_BP_ACC d
ON d.COD_ENTITY = a.ENTIDAD AND d.COD_BLCE_PROD = a.PRODUCTO_BALANCE AND d.COD_CURRENCY = a.DIVISA AND d.COD_GL_GROUP = a.AGRUPADOR_CONTABLE
LEFT JOIN MIS_PAR_CAT_BP e
ON a.PRODUCTO_BALANCE = e.COD_BLCE_PROD
LEFT JOIN MIS_PAR_REL_PL_ACC f
ON f.COD_ENTITY = a.ENTIDAD AND f.COD_PL_ACC = a.CUENTA_PYG AND f.COD_CURRENCY = a.DIVISA AND f.COD_GL_GROUP = a.AGRUPADOR_CONTABLE
LEFT JOIN MIS_PAR_CAT_PL g
ON a.CUENTA_PYG = g.COD_PL_ACC
LEFT JOIN MIS_PAR_REL_BL_ACC h
ON h.COD_ENTITY = a.ENTIDAD AND h.COD_GL_GROUP = a.AGRUPADOR_CONTABLE AND h.COD_BUSINESS_LINE = a.LINEA_NEGOCIO
LEFT JOIN MIS_PAR_CAT_BL i
ON i.COD_BUSINESS_LINE = a.LINEA_NEGOCIO
LEFT JOIN (SELECT * FROM MIS_PAR_EXCH_RATE WHERE DATA_DATE = '${var:periodo}' AND COD_CONT = 'TC_FOTO') j   
ON a.DATA_DATE=j.DATA_DATE AND a.DIVISA=j.COD_CURRENCY
AND IF(ISNULL(TRIM(j.COD_ENTITY),'') = '' ,'1',a.ENTIDAD) = IF(ISNULL(TRIM(j.COD_ENTITY),'') = '','1',j.COD_ENTITY)
WHERE a.DATA_DATE='${var:periodo}' AND TRIM(a.AGRUPADOR_CONTABLE) != '' AND TRIM(a.CUENTA_PYG) != '' AND TRIM(a.PRODUCTO_BALANCE) != '' AND TRIM(a.LINEA_NEGOCIO) != '' 
    AND TRIM(a.ENTIDAD) != '' AND TRIM(a.DIVISA) != '' 
;