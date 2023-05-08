--------------------------------------------------------------- MIS_LIABILITIES -----------------------------------------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_ACMST; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/ACMST.CSV' INTO TABLE MIS_LOAD_ACMST;

TRUNCATE TABLE IF EXISTS MIS_LOAD_CNTRLRTE; 
LOAD DATA INPATH '${var:ruta_fuentes_pasivos}/CNTRLRTE.CSV' INTO TABLE MIS_LOAD_CNTRLRTE;

--- Eliminación de Información Previa ---

ALTER TABLE MIS_APR_LIABILITIES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');


-------------------------------------------- PLAZO --------------------------------------------

----Aprovisionamiento de Pasivos Capital
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, deagln AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,  
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, CAST((a.DEAPRI * (-1))as DECIMAL(30,10)) AS EOPBAL_CAP, 
    NULL AS EOPBAL_INT, CAST((a.DEAAVP * (-1)) as DECIMAL(30,10)) AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'DEALS' AS COD_INFO_SOURCE 
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.DEACUN AS string) 
WHERE a.DEABNK <> '02' AND (TRIM(a.DEATYP) = 'TDS' OR TRIM(a.DEAPRO)='FBCO' ) AND TRIM(a.DEASTS) <> 'C';


----Aprovisionamiento de Pasivos Intereses
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, 
    CAST((a.DEAMEI) * (-1) as DECIMAL(30,10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'DEALS' AS COD_INFO_SOURCE
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON TRIM(CAST(a.DEAACC AS string)) = B.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.DEACUN AS string) 
WHERE a.DEABNK <> '02' AND (TRIM(a.DEATYP) = 'TDS' OR TRIM(a.DEAPRO)='FBCO' ) AND TRIM(a.DEASTS) <> 'C';


/*----Aprovisionamiento de Pasivos Resultados
INSERT INTO MIS_APR_LIABILITIES 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, 
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, a.DEAINM AS PL, 'DEALS' AS COD_INFO_SOURCE 
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.DEACUN AS string) 
WHERE a.DEABNK <> '02' AND (TRIM(a.DEATYP) = 'TDS' OR TRIM(a.DEAPRO)='FBCO' ) AND TRIM(a.DEASTS) <> 'C';*/

----Aprovisionamiento de Pasivos Resultados (DLAHI)
DROP TABLE IF EXISTS MIS_TMP_LIABILITIES_DLH;
CREATE TABLE MIS_TMP_LIABILITIES_DLH AS 
SELECT TRIM(DLAACC) AS DLAACC, TRIM(DLAGLN) AS DLAGLN, TRIM(DLACCN) AS DLACCN, TRIM(DLACCY) AS DLACCY, TRIM(DLABNK) AS DLABNK, TRIM(DLAPRO) AS DLAPRO, case when TRIM(dladcc) = 'D' then sum(isnull(DLAAMT,0)) end as debito, case when TRIM(dladcc) = 'C' then sum(isnull(DLAAMT,0)) end as credito FROM MIS_LOAD_DLAHI 
WHERE dlabyy = substr('${var:periodo}',3,2) and RIGHT(CONCAT('00',dlabmm),2) = substr('${var:periodo}',5,2)
and trim(dlagln) not in (select distinct cod_gl_group from mis_par_rel_exc_dlahi)
GROUP BY DLAACC, DLAGLN, DLACCN, DLACCY, DLABNK, DLAPRO, DLADCC
;

--COMPUTE STATS MIS_TMP_LIABILITIES_DLH;

INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
NULL AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, 
CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            ELSE TRIM(d.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
TRIM(d.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(d.DEAICD AS string), '') AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, 
NULL AS AVGBAL_INT, CAST(SUM(isnull(a.DEBITO,0) - isnull(a.CREDITO,0)) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_LIABILITIES_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = b.IDF_CTO
LEFT JOIN MIS_LOAD_DEALS d
ON a.DLAACC = CAST(d.DEAACC AS string)
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(d.DEACUN AS string)
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_LIABILITIES WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_CONT = 'PLZ') z
ON TRIM(a.DLAACC) = TRIM(z.IDF_CTO)
WHERE d.DEATYP IS NOT NULL
GROUP BY a.DLAACC, a.DLAGLN, a.DLACCN, a.DLACCY, a.DLABNK, b.IDF_CTO, b.COD_PRODUCT, TRIM(d.DEATYP), cl.COD_SECTOR, d.DEAPRO, d.DEAICD
;
/*
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PLZ' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
NULL AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_LIABILITIES_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON TRIM(a.DLAACC) = TRIM(b.IDF_CTO)
LEFT JOIN (SELECT a.* FROM MIS_APR_LIABILITIES a LEFT JOIN (select * from MIS_APR_LIABILITIES where COD_CONT = 'PLZ' AND TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1)) b ON a.idf_cto = b.idf_cto and a.cod_value = b.cod_value 
WHERE a.data_date = '${var:periodo}' and a.cod_value = 'CAP' AND a.COD_CONT = 'PLZ' and b.idf_cto is null) d
ON d.DATA_DATE = '${var:periodo}' AND TRIM(a.DLAACC) = TRIM(d.IDF_CTO) AND TRIM(a.DLACCY) = TRIM(d.COD_CURRENCY) AND TRIM(a.DLABNK) = TRIM(d.COD_ENTITY)
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_LIABILITIES WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_CONT = 'PLZ') z
ON a.DLAACC = z.IDF_CTO
WHERE d.COD_PRODUCT IS NOT NULL
;
*/
-------------------------------------------- CUENTAS --------------------------------------------

----Aprovisionamiento de Pasivos Capital
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'CTA' AS COD_CONT, TRIM(a.ACMACC) AS IDF_CTO, acmgln AS COD_GL, NULL AS DES_GL, TRIM(a.ACMCCN) AS COD_ACCO_CENT, TRIM(a.ACMBRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
    'CAP' AS COD_VALUE, TRIM(a.ACMCCY) AS COD_CURRENCY, TRIM(a.ACMBNK) AS COD_ENTITY, 
    CONCAT(TRIM(a.ACMATY), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.ACMPRO) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    CAST(a.ACMNBL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    CAST(a.ACMNAV AS decimal(30, 10)) AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'ACMST' AS COD_INFO_SOURCE
FROM MIS_LOAD_ACMST a 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.ACMCUN AS string) 
LEFT JOIN (select distinct cod_entity, cod_currency, cod_subproduct, cod_gl_group, strleft(cod_blce_prod,length(cod_blce_prod)-2) as cod_blce_prod from mis_par_rel_bp_oper) c
ON acmgln = cod_gl_group and acmpro = c.cod_subproduct and acmbnk = c.cod_entity and acmccy = c.cod_currency
WHERE a.ACMBNK <> '02' and (ACMNBL <= 0 or (ACMNBL > 0 and c.cod_blce_prod like '%DISPONIBLE%'));

---- Sobregiros ----
INSERT INTO MIS_APR_ASSETS
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'PREST' AS COD_CONT, TRIM(a.ACMACC) AS IDF_CTO, CAST(b.GLMXOD AS STRING) AS COD_GL, NULL AS DES_GL, TRIM(a.ACMCCN) AS COD_ACCO_CENT, TRIM(a.ACMBRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
    'CAP' AS COD_VALUE, TRIM(a.ACMCCY) AS COD_CURRENCY, TRIM(a.ACMBNK) AS COD_ENTITY, 
    'SOB' AS COD_PRODUCT, TRIM(a.ACMPRO) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    CAST(a.ACMNBL AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    CAST(a.ACMNAV AS decimal(30, 10)) AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'ACMST' AS COD_INFO_SOURCE 
FROM MIS_LOAD_ACMST a 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.ACMCUN AS string) 
LEFT JOIN MIS_LOAD_GLMST b
ON a.acmgln = b.glmgln and a.acmbnk = b.glmbnk
LEFT JOIN (select distinct cod_entity, cod_currency, cod_subproduct, cod_gl_group, strleft(cod_blce_prod,length(cod_blce_prod)-2) as cod_blce_prod from mis_par_rel_bp_oper) c
ON a.acmgln = cod_gl_group and a.acmpro = c.cod_subproduct and a.acmbnk = c.cod_entity and a.acmccy = c.cod_currency
WHERE a.ACMBNK <> '02' and (ACMNBL > 0 and c.cod_blce_prod not like '%DISPONIBLE%')
;

----Aprovisionamiento de Pasivos Intereses
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'CTA' AS COD_CONT, TRIM(a.ACMACC) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, TRIM(a.ACMCCN) AS COD_ACCO_CENT, TRIM(a.ACMBRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
    'INT' AS COD_VALUE, TRIM(a.ACMCCY) AS COD_CURRENCY, TRIM(a.ACMBNK) AS COD_ENTITY, 
    CONCAT(TRIM(a.ACMATY), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.ACMPRO) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    CAST((a.ACMIAC * -1) AS decimal(30, 10)) AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'ACMST' AS COD_INFO_SOURCE
FROM MIS_LOAD_ACMST a 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.ACMCUN AS string) 
WHERE a.ACMBNK <> '02';

/*----Aprovisionamiento de Pasivos Resultados
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'CTA' AS COD_CONT, TRIM(a.ACMACC) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, TRIM(a.ACMCCN) AS COD_ACCO_CENT, TRIM(a.ACMBRN) AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
    'RSL' AS COD_VALUE, TRIM(a.ACMCCY) AS COD_CURRENCY, TRIM(a.ACMBNK) AS COD_ENTITY, 
    CONCAT(TRIM(a.ACMATY), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.ACMPRO) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, a.ACMIPL AS PL, 'ACMST' AS COD_INFO_SOURCE
FROM MIS_LOAD_ACMST a 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.ACMCUN AS string) 
WHERE a.ACMBNK <> '02';*/

----Aprovisionamiento de Activos Resultados (DLAHI)
INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'CTA' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
NULL AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_LIABILITIES_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = b.IDF_CTO
LEFT JOIN (SELECT a.* FROM MIS_APR_LIABILITIES a LEFT JOIN (select * from MIS_APR_LIABILITIES where COD_CONT = 'CTA' AND TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1)) b ON a.idf_cto = b.idf_cto and a.cod_value = b.cod_value 
WHERE a.data_date = '${var:periodo}' and a.cod_value = 'CAP' AND a.COD_CONT = 'CTA' and b.idf_cto is null) d
ON d.DATA_DATE = '${var:periodo}' AND a.DLAACC = d.IDF_CTO AND a.DLACCY = d.COD_CURRENCY AND a.DLABNK = d.COD_ENTITY
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_LIABILITIES WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_CONT = 'CTA') z
ON a.DLAACC = z.IDF_CTO
WHERE d.COD_PRODUCT IS NOT NULL
;

INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'CTA' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
NULL AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_LIABILITIES_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = b.IDF_CTO
LEFT JOIN (SELECT distinct cod_cont, idf_cto, cod_value, cod_currency, cod_entity, cod_product, cod_subproduct, cod_act_type, data_date FROM MIS_APR_LIABILITIES WHERE idf_cto not in 
(select distinct idf_cto from mis_apr_assets where data_date = '${var:periodo}' and cod_product = 'SOB')) d
ON TO_TIMESTAMP(d.DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1) AND a.DLAACC = d.IDF_CTO AND a.DLACCY = d.COD_CURRENCY AND a.DLABNK = d.COD_ENTITY AND d.COD_VALUE = 'CAP' AND d.COD_CONT = 'CTA'
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_LIABILITIES WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_CONT = 'CTA') z
ON a.DLAACC = z.IDF_CTO
WHERE d.COD_PRODUCT IS NOT NULL
;

INSERT INTO MIS_APR_LIABILITIES
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'CTA' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
NULL AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, 
d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_LIABILITIES_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = b.IDF_CTO
LEFT JOIN (SELECT distinct cod_cont, idf_cto, cod_currency, cod_entity, cod_product, cod_subproduct, cod_act_type, data_date FROM MIS_APR_LIABILITIES WHERE idf_cto in 
(select distinct idf_cto from mis_apr_assets where data_date = '${var:periodo}' and cod_product = 'SOB')) d
ON TO_TIMESTAMP(d.DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1) AND a.DLAACC = d.IDF_CTO AND a.DLACCY = d.COD_CURRENCY AND a.DLABNK = d.COD_ENTITY AND d.COD_CONT = 'CTA'
WHERE d.COD_PRODUCT IS NOT NULL
;

ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='ACMST');

----Aprovisionamiento de Contratos asociados a Pasivos
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='ACMST') 
SELECT DISTINCT
    TRIM(a.ACMACC) AS IDF_CTO, TRIM(a.ACMBNK) AS COD_ENTITY, 
    CONCAT(TRIM(a.ACMATY), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.ACMPRO) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    TRIM(a.ACMCCY) AS COD_CURRENCY, TRIM(a.ACMCUN) AS IDF_CLI, TRIM(a.ACMCCN) AS COD_ACCO_CENT, TRIM(a.ACMBRN) AS COD_OFFI, NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, 
CASE WHEN ACMNBL > 0 and c.cod_blce_prod not like '%DISPONIBLE%' THEN CAST(a.ACMOI1 + d.RTEFRA AS decimal(30, 10))
     ELSE CAST(CASE WHEN ACMNBL * -1 < RTEMKL then 0
               WHEN ACMNBL * -1 >= RTEMKL and (ACMNBL * -1 <= RTEMB1 OR RTEMB1 = 0) then RTEMKR
               WHEN ACMNBL * -1 >= RTEMB2 and (ACMNBL * -1 <= RTEMB3 OR RTEMB3 = 0) then RTEMR2
               WHEN ACMNBL * -1 >= RTEMB4 and (ACMNBL * -1 <= RTEMB5 OR RTEMB5 = 0) then RTEMR4
               WHEN ACMNBL * -1 >= RTEMB6 and (ACMNBL * -1 <= RTEMB7 OR RTEMB7 = 0) then RTEMR6
               WHEN ACMNBL * -1 >= RTEMB8 and (ACMNBL * -1 <= RTEMB9 OR RTEMB9 = 0) then RTEMR8
               WHEN ACMNBL * -1 >= RTEMBX and (ACMNBL * -1 <= RTEMBY OR RTEMBY = 0) then RTEMRX
          ELSE RTEMRY END AS decimal(30, 10))
END AS RATE_INT, --SUMAR RTEFRA
    CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.ACMOPY 
                THEN CONCAT('19', LPAD(CAST(a.ACMOPY AS string), 2, '0')) 
                ELSE CONCAT('2', LPAD(CAST(a.ACMOPY AS string), 3, '0')) END, 
           LPAD(CAST(a.ACMOPM AS string), 2, '0'), LPAD(CAST(a.ACMOPD  AS string), 2, '0')) AS DATE_ORIGIN, 
    NULL AS DATE_LAST_REV, 
    NULL AS DATE_PRX_REV, NULL AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, NULL AS INI_AM, NULL AS CUO_AM, NULL AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, TRIM(a.ACMSCH) AS IND_CHANNEL, NULL AS COD_TYP_LIC,
    TRIM(a.ACMSST) AS COD_SELLER, TRIM(b.CNODSC) AS DES_SELLER, NULL AS COU_CAR_OFF, NULL AS COD_CONV, a.ACMSST AS COD_EXEC_CTO, CASE WHEN cov.IDF_CTO IS NULL THEN 'N' ELSE 'Y' END AS COD_COVID_PORT, 
    CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.ACMOPY 
                THEN CONCAT('19', LPAD(CAST(a.ACMOPY AS string), 2, '0')) 
                ELSE CONCAT('2', LPAD(CAST(a.ACMOPY AS string), 3, '0')) END, 
           LPAD(CAST(a.ACMOPM AS string), 2, '0'), LPAD(CAST(a.ACMOPD  AS string), 2, '0')) AS DATE_DISB, NULL AS CARD_NUMBER, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_ACMST a 
LEFT JOIN (SELECT CNOCFL, CNORCD, MAX(CNODSC) AS CNODSC FROM MIS_LOAD_CNOFC WHERE TRIM(CNOCFL) = '65' GROUP BY CNORCD, CNOCFL) b
ON TRIM(a.ACMSST) = TRIM(b.CNORCD)
/*LEFT JOIN (SELECT CNOCFL, CNORCD, MAX(CNODSC) AS CNODSC FROM MIS_LOAD_CNOFC WHERE TRIM(CNOCFL) = '62' GROUP BY CNORCD, CNOCFL) cha
ON TRIM(a.ACMSCH) = TRIM(cha.CNORCD)*/
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.ACMCUN AS string) 
LEFT JOIN (select distinct cod_entity, cod_currency, cod_subproduct, cod_gl_group, strleft(cod_blce_prod,length(cod_blce_prod)-2) as cod_blce_prod from mis_par_rel_bp_oper) c
ON a.acmgln = c.cod_gl_group and a.acmpro = c.cod_subproduct and a.acmbnk = c.cod_entity and a.acmccy = c.cod_currency
LEFT JOIN MIS_LOAD_CNTRLRTE d
ON rtekey = concat(ACMBNK, ACMATY, if(length(ACMACL)=1,concat('0',ACMACL),ACMACL))
LEFT JOIN (SELECT * FROM MIS_PAR_REL_CAR_COVID WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6)) cov
ON TRIM(a.ACMACC) = cov.IDF_CTO
WHERE a.ACMBNK <> '02';