--------------------------------------------------------------- ASIG. DIM. OPERACIONAL -----------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Inserción de registros en la tabla de dimensiones DWH_ASSETS
ALTER TABLE MIS_DWH_${var:tabla}_M
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_DWH_${var:tabla}_M PARTITION (DATA_DATE, COD_CONT)
SELECT cod_gl, des_gl, cod_acco_cent, cod_expense, cod_offi, cod_nar, cod_blce_status, cod_currency, cod_entity, eopbal_cap, eopbal_int, avgbal_cap, avgbal_int, pl, cod_info_source, cod_gl_group, des_gl_group, account_concept, cod_pl_acc, des_pl_acc, cod_blce_prod, des_blce_prod, cod_business_line, des_business_line, ftp, ftp_result, ind_pool, cod_segment, des_segment, exp_type, exp_family, null as cod_driver, null as cod_acco_cent_origin, null as cod_expense_origin, data_date, cod_cont
FROM MIS_DWH_${var:tabla}
WHERE DATA_DATE = '${var:periodo}' AND IND_POOL IS NULL AND COD_CONT = 'EXP';