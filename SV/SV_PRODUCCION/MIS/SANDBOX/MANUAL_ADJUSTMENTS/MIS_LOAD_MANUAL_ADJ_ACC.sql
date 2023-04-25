--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Ajustes Manuales Contables ---
TRUNCATE TABLE MIS_DWH_MANUAL_ADJ_ACC;
LOAD DATA INPATH '${var:ruta_fuentes_ajustes}/Ajustes_Manuales_Contables.csv' INTO TABLE MIS_DWH_MANUAL_ADJ_ACC;


---Registros que no cumplen con la validación (La llave Entidad - Agrupador contable - Divisa, no existe)
INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'AGRUPADOR CONTABLE', '', A.agrupador_contable, STRRIGHT(CONCAT('0', A.entidad),2), A.divisa, '', '', A.producto_balance, A.cuenta_pyg, '', '', '', A.importe_ml, A.importe_mo, A.saldo_medio_ml, A.saldo_medio_mo
FROM (SELECT * 
      FROM mis_dwh_manual_adj_acc AS A
      WHERE A.contenido = 'Contable' AND NOT EXISTS (SELECT 1 
                                                     FROM mis_par_rel_caf_acc 
                                                     WHERE cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND cod_gl_group = A.agrupador_contable AND cod_currency = A.divisa)) AS A;

INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'PRODUCTO DE BALANCE', '', A.agrupador_contable, STRRIGHT(CONCAT('0', A.entidad),2), A.divisa, '', '', A.producto_balance, A.cuenta_pyg, '', '', '', A.importe_ml, A.importe_mo, A.saldo_medio_ml, A.saldo_medio_mo
FROM (SELECT * 
      FROM mis_dwh_manual_adj_acc AS A
      WHERE A.contenido = 'Contable' AND NOT EXISTS (SELECT 1 
                                                     FROM mis_par_rel_bp_acc 
                                                     WHERE cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND cod_blce_prod = A.producto_balance AND cod_currency = A.divisa)) AS A;

INSERT INTO MIS_DWH_MANUAL_ADJ_OPER_REJECTIONS
SELECT 'CONT', 'CUENTA GESTION', '', A.agrupador_contable, STRRIGHT(CONCAT('0', A.entidad),2), A.divisa, '', '', A.producto_balance, A.cuenta_pyg, '', '', '', A.importe_ml, A.importe_mo, A.saldo_medio_ml, A.saldo_medio_mo
FROM (SELECT * 
      FROM mis_dwh_manual_adj_acc AS A
      WHERE A.contenido = 'Contable' AND NOT EXISTS (SELECT 1 
                                                     FROM mis_par_rel_pl_acc 
                                                     WHERE cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND cod_pl_acc = A.cuenta_pyg 
 AND cod_currency = A.divisa)) AS A;

---Registros que cumplen con la validación (La llave Entidad - Agrupador contable - Divisa, existe)
INSERT INTO MIS_DM_BALANCE_RESULT
(COD_CONT,IDF_CTO,COD_GL,DES_GL,COD_ACCO_CENT,COD_OFFI,COD_NAR,COD_BLCE_STATUS,COD_CURRENCY,COD_ENTITY,COD_PRODUCT,COD_SUBPRODUCT,EOPBAL_TOT,
EOPBAL_CAP,EOPBAL_INT,AVGBAL_TOT,AVGBAL_CAP,AVGBAL_INT,PL,COD_INFO_SOURCE,COD_TYP_FEE,IDF_CLI,TYP_DOC_ID,NUM_DOC_ID,NOM_NAME,NOM_LASTN,COD_RES_COUNT,
COD_TYP_CLNT,COD_COND_CLNT,COD_ENG,VAL_SEX,COD_SEGMENT,DES_SEGMENT,IND_EMPL_GRUP,DATE_CLNT_REG,DATE_CLNT_WITHDRAW,COD_MANAGER,COD_SECTOR,DES_SECTOR,
RATING_CLI,COD_BCA_INT,COD_AMRT_MET,COD_RATE_TYPE,RATE_INT,DATE_ORIGIN,DATE_LAST_REV,DATE_PRX_REV,EXP_DATE,FREQ_INT_PAY,COD_UNI_FREQ_INT_PAY,
FRE_REV_INT,COD_UNI_FRE_REV_INT,AMRT_TRM,COD_UNI_AMRT_TRM,INI_AM,CUO_AM,CREDIT_LIM_AM,PREDEF_RATE_IND,PREDEF_RATE,IND_CHANNEL,PL_TOT,AVGBAL_TOT_YTD,
PL_YTD,FTP_RESULT_YTD,PL_TOT_YTD,EOPBAL_TOT_RPT,AVGBAL_TOT_RPT,PL_TOT_RPT,PL_RPT,FTP_RESULT_RPT,AVGBAL_TOT_YTD_RPT,PL_YTD_RPT,FTP_RESULT_YTD_RPT,
PL_TOT_YTD_RPT,COD_GL_GROUP,DES_GL_GROUP,ACCOUNT_CONCEPT,COD_PL_ACC,DES_PL_ACC,COD_BLCE_PROD,DES_BLCE_PROD,COD_BUSINESS_LINE,DES_BUSINESS_LINE,
FTP,FTP_RESULT,IND_POOL,COD_SUBSEGMENT,COD_GROUP_EC)
PARTITION (DATA_DATE)
SELECT 'RCTB', '', '', '', '', '', '', '', A.divisa, STRRIGHT(CONCAT('0', A.entidad),2), '', '', CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.importe_mo, LENGTH(A.importe_mo)-3), '.', ''), ',', ''), STRRIGHT(A.importe_mo,3)) AS DECIMAL(30,2)), 
CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.importe_mo, LENGTH(A.importe_mo)-3), '.', ''), ',', ''), STRRIGHT(A.importe_mo,3)) AS DECIMAL(30,2)), 
NULL, CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.saldo_medio_mo, LENGTH(A.saldo_medio_mo)-3), '.', ''), ',', ''), STRRIGHT(A.saldo_medio_mo,3)) AS DECIMAL(30,2)),  
CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.saldo_medio_mo, LENGTH(A.saldo_medio_mo)-3), '.', ''), ',', ''), STRRIGHT(A.saldo_medio_mo,3)) AS DECIMAL(30,2)), NULL, NULL, 
CONCAT('AJUSTE_',A.criterio_de_ajuste), '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', NULL, '', '', '', '',  NULL, '', NULL, '', 
NULL, '', NULL, NULL, NULL, '', NULL, '', NULL, NULL, NULL, NULL, NULL, CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.importe_ml, LENGTH(A.importe_ml)-3), '.', ''), ',', ''), STRRIGHT(A.importe_ml,3)) AS DECIMAL(30,2)), 
CAST(CONCAT(TRANSLATE(TRANSLATE(STRLEFT(A.saldo_medio_ml, LENGTH(A.saldo_medio_ml)-3), '.', ''), ',', ''), STRRIGHT(A.saldo_medio_ml,3)) AS DECIMAL(30,2)), NULL, NULL, NULL, NULL, NULL, NULL, NULL, A.agrupador_contable,  C.des_gl_group, '', A.cuenta_pyg, G.DES_PL_ACC, A.producto_balance, E.DES_BLCE_PROD, A.linea_negocio, I.DES_BUSINESS_LINE, NULL, NULL, '', '', '', '${var:periodo}' 
FROM mis_dwh_manual_adj_acc AS A 
inner join mis_par_rel_caf_acc AS B
on B.cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND B.cod_gl_group = A.agrupador_contable AND B.cod_currency = A.divisa
LEFT JOIN mis_par_cat_caf AS C ON A.agrupador_contable = C.cod_gl_group
inner join mis_par_rel_bp_acc AS D
ON D.cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND D.cod_blce_prod = A.producto_balance AND D.cod_currency = A.divisa AND D.cod_gl_group = A.agrupador_contable
LEFT JOIN MIS_PAR_CAT_BP E ON A.producto_balance = E.COD_BLCE_PROD
inner join mis_par_rel_pl_acc AS F
ON F.cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND F.cod_pl_acc = A.cuenta_pyg AND F.cod_currency = A.divisa and F.cod_gl_group = A.agrupador_contable
LEFT JOIN MIS_PAR_CAT_PL G ON A.cuenta_pyg = G.COD_PL_ACC
LEFT JOIN mis_par_rel_bl_acc H 
ON H.cod_entity = STRRIGHT(CONCAT('0', A.entidad),2) AND H.COD_GL_GROUP = A.agrupador_contable AND H.COD_BUSINESS_LINE = A.linea_negocio
LEFT JOIN mis_par_cat_bl I ON I.COD_BUSINESS_LINE = A.linea_negocio
WHERE A.contenido = 'Contable'
;