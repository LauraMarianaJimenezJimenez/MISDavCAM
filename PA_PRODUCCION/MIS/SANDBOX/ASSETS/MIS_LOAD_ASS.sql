----------------------------------------------------------- MIS_APR_ASSETS -----------------------------------------------------------

--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--COMPUTE STATS MIS_APR_ASSETS;
--COMPUTE STATS MIS_APR_CONTRACT_DT;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_DEALS; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/DEALS.CSV' INTO TABLE MIS_LOAD_DEALS;
TRUNCATE TABLE IF EXISTS MIS_LOAD_AMOFE; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/AMOFE.CSV' INTO TABLE MIS_LOAD_AMOFE;
TRUNCATE TABLE IF EXISTS MIS_LOAD_S3STRA; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/S3STRA.CSV' INTO TABLE MIS_LOAD_S3STRA;
TRUNCATE TABLE IF EXISTS MIS_LOAD_SGWTRA; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/SGWTRA.CSV' INTO TABLE MIS_LOAD_SGWTRA;
TRUNCATE TABLE IF EXISTS MIS_LOAD_DLAHI; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/DLAHI.CSV' INTO TABLE MIS_LOAD_DLAHI;
TRUNCATE TABLE IF EXISTS MIS_LOAD_S3NTRA; 
LOAD DATA INPATH '${var:ruta_fuentes_activos}/S3NTRA.CSV' INTO TABLE MIS_LOAD_S3NTRA;

ALTER TABLE MIS_APR_ASSETS
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--COMPUTE STATS MIS_LOAD_DEALS;
COMPUTE INCREMENTAL STATS MIS_APR_CLIENT_DT partition (data_date = '${var:periodo}');

----Aprovisionamiento de Activos Capital
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, deagln AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            WHEN a.DEAICD = 2101 THEN 'VIV'
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, a.DEAPRI AS EOPBAL_CAP, 
    NULL AS EOPBAL_INT, a.DEAAVP AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'DEALS' AS COD_INFO_SOURCE 
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND TRIM(cl.IDF_CLI) = TRIM(CAST(a.DEACUN AS string))
WHERE a.DEABNK <> '02' AND TRIM(a.DEATYP) <> 'TDS' AND a.DEASTS <> 'C' AND TRIM(a.DEAPRO) <> 'FBCO' AND ((TRIM(DEATYP) = 'PLP' AND DEAPRI > 0) OR TRIM(DEATYP) <> 'PLP');


----Aprovisionamiento de Activos Intereses
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'INT' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            WHEN a.DEAICD = 2101 THEN 'VIV'
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, 
    CASE WHEN a.DEAMEI < 0 THEN 0 ELSE a.DEAMEI END AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'DEALS' AS COD_INFO_SOURCE
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND TRIM(cl.IDF_CLI) = TRIM(CAST(a.DEACUN AS string))
WHERE a.DEABNK <> '02' AND TRIM(a.DEATYP) <> 'TDS' AND a.DEASTS <> 'C' AND TRIM(a.DEAPRO) <> 'FBCO' AND ((TRIM(DEATYP) = 'PLP' AND DEAPRI > 0) OR TRIM(DEATYP) <> 'PLP');

--COMPUTE STATS MIS_LOAD_DLAHI;

DROP TABLE IF EXISTS MIS_TMP_ASSETS_DLH;
CREATE TABLE MIS_TMP_ASSETS_DLH AS 
SELECT DLAACC, DLAGLN, DLACCN, DLACCY, DLABNK, case when dladcc = 'D' then sum(isnull(DLAAMT,0)) end as debito, case when dladcc = 'C' then sum(isnull(DLAAMT,0)) end as credito FROM MIS_LOAD_DLAHI 
WHERE dlabyy = substr('${var:periodo}',3,2) and RIGHT(CONCAT('00',dlabmm),2) = substr('${var:periodo}',5,2)
and trim(dlagln) not in (select distinct cod_gl_group from mis_par_rel_exc_dlahi)
GROUP BY DLAACC, DLAGLN, DLACCN, DLACCY, DLABNK, DLADCC
;

--COMPUTE STATS MIS_TMP_ASSETS_DLH;
COMPUTE INCREMENTAL STATS MIS_APR_ASSETS partition (data_date = '${var:periodo}');
COMPUTE INCREMENTAL STATS MIS_APR_CONTRACT_DT partition (data_date = '${var:periodo}');

----Aprovisionamiento de Activos Resultados (DLAHI)
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
d.COD_OFFI AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_ASSETS_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = B.IDF_CTO
LEFT JOIN MIS_APR_CONTRACT_DT d
ON TO_TIMESTAMP(d.DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1) AND a.DLAACC = d.IDF_CTO AND a.DLACCY = d.COD_CURRENCY AND a.DLABNK = d.COD_ENTITY
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_ASSETS WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_PRODUCT <> 'SOB') z
ON a.DLAACC = z.IDF_CTO
WHERE d.COD_PRODUCT IS NOT NULL
;

INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}')
SELECT 'PREST' AS COD_CONT, a.DLAACC AS IDF_CTO, a.DLAGLN AS COD_GL, NULL AS DES_GL, a.DLACCN AS COD_ACCO_CENT, 
d.COD_OFFI AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DLACCY AS COD_CURRENCY, a.DLABNK AS COD_ENTITY, CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT ELSE d.COD_PRODUCT END AS COD_PRODUCT, d.COD_SUBPRODUCT AS COD_SUBPRODUCT, d.COD_ACT_TYPE AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
CAST(isnull(a.DEBITO,0) - isnull(a.CREDITO,0) AS DECIMAL(30,10)) AS PL, 'DLAHI' AS COD_INFO_SOURCE 
FROM MIS_TMP_ASSETS_DLH a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON a.DLAACC = B.IDF_CTO
LEFT JOIN (SELECT a.* FROM MIS_APR_ASSETS a LEFT JOIN (select * from MIS_APR_ASSETS where TO_TIMESTAMP(DATA_DATE, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1)) b ON a.idf_cto = b.idf_cto and a.cod_value = b.cod_value 
WHERE a.data_date = '${var:periodo}' and a.cod_value = 'CAP' and b.idf_cto is null) d
ON d.DATA_DATE = '${var:periodo}' AND a.DLAACC = d.IDF_CTO AND a.DLACCY = d.COD_CURRENCY AND a.DLABNK = d.COD_ENTITY
INNER JOIN (SELECT DISTINCT IDF_CTO FROM MIS_APR_ASSETS WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6) AND COD_VALUE = 'CAP' AND COD_PRODUCT <> 'SOB') z
ON a.DLAACC = z.IDF_CTO
WHERE d.cod_product IS NOT NULL
;

/*----Aprovisionamiento de Activos Resultados
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            WHEN a.DEAICD = 2101 THEN 'VIV'
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
WHERE a.DEABNK <> '02' AND TRIM(a.DEATYP) <> 'TDS' AND a.DEASTS <> 'C';*/


----Aprovisionamiento de Activos Inversiones Perdidas y Ganancias Acumuladas
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CAST(a.DEAACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'PGA' AS COD_VALUE, a.DEACCY AS COD_CURRENCY, a.DEABNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            WHEN a.DEAICD = 2101 THEN 'VIV'
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, a.DEAXRA AS EOPBAL_CAP,   
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'DEALS' AS COD_INFO_SOURCE
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.DEACUN AS string)
WHERE a.DEABNK <> '02' AND a.DEAXRA <> 0 AND a.DEASTS <> 'C'
AND ((TRIM(DEATYP) = 'PLP' AND DEAPRI > 0 AND TRIM(a.DEAPRO) NOT IN ('BPEV','BBEV') ) OR TRIM(DEATYP) <> 'PLP')
;

--COMPUTE STATS MIS_LOAD_AMOFE;

----Aprovisionamiento de Activos Inversiones Descuentos
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CAST(a.AMOACC AS string) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, CAST(a.AMOCCN AS string) AS COD_ACCO_CENT, 
    TRIM(b.DEABRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, CASE WHEN b.deaham/1000000 < 1 AND TRIM(b.DEATYP) = 'PLP' AND (a.AMOACA * -1) < 0 THEN 'DESC' ELSE 'PRI' END AS COD_VALUE, a.AMOCCY AS COD_CURRENCY, a.AMOBNK AS COD_ENTITY,
    CONCAT(
        CASE WHEN c.IDF_CTO IS NOT NULL THEN c.COD_PRODUCT
            WHEN b.DEAICD = 2101 THEN 'VIV'
            ELSE TRIM(b.DEATYP) END,   
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    TRIM(b.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(b.DEAICD AS string), '') AS COD_ACT_TYPE, CASE WHEN b.deaham/1000000 < 1 AND TRIM(b.DEATYP) = 'PLP' THEN CAST(a.AMOACA * -1 AS DECIMAL(30,10)) ELSE a.AMOACA END AS EOPBAL_CAP,   
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'AMOFE' AS COD_INFO_SOURCE
FROM MIS_LOAD_AMOFE a
INNER JOIN (SELECT * FROM MIS_LOAD_DEALS WHERE ((TRIM(DEATYP) = 'PLP' AND DEAPRI > 0) OR TRIM(DEATYP) <> 'PLP')) b
ON TRIM(a.AMOACC) = TRIM(CAST(b.DEAACC AS string)) AND TRIM(a.AMOBNK) = TRIM(b.DEABNK)
LEFT JOIN MIS_PAR_REL_PROD_SPE c 
ON a.AMOACC = c.IDF_CTO 
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(b.DEACUN AS string)
WHERE a.AMOBNK <> '02' AND a.AMOACA <> 0;


ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='DEALS');

----Aprovisionamiento de Contratos asociados a Activos
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='DEALS') 
SELECT DISTINCT CAST(a.DEAACC AS string) AS IDF_CTO, a.DEABNK AS COD_ENTITY, 
    CONCAT(
        CASE WHEN b.IDF_CTO IS NOT NULL THEN b.COD_PRODUCT
            WHEN a.DEAICD = 2101 THEN 'VIV'
            ELSE TRIM(a.DEATYP) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(a.DEAPRO) AS COD_SUBPRODUCT, IFNULL(CAST(a.DEAICD AS string), '') AS COD_ACT_TYPE, 
    a.DEACCY AS COD_CURRENCY, CAST(a.DEACUN AS string) AS IDF_CLI, CAST(a.DEACCN AS string) AS COD_ACCO_CENT, 
    TRIM(a.DEABRN) AS COD_OFFI, NULL AS COD_BCA_INT, a.DEAPPD AS COD_AMRT_MET, 
    CASE WHEN a.DEAFTB = 'TA' OR TRIM(a.DEAFTB) = '' THEN 'F' --TODO:  a.DEAFTP='' AND 
        ELSE 'V' 
        END AS COD_RATE_TYPE, 
    CAST((a.DEARTE/100) + (a.DEAFRT/100) AS decimal(30, 10)) AS RATE_INT, 
    CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.DEAODY 
                THEN CONCAT('19', LPAD(CAST(a.DEAODY AS string), 2, '0')) 
                ELSE CONCAT('2', LPAD(CAST(a.DEAODY AS string), 3, '0')) END, 
        LPAD(CAST(a.DEAODM AS string), 2, '0'), LPAD(CAST(a.DEAODD  AS string), 2, '0')) AS DATE_ORIGIN, 
    CASE WHEN a.DEAFTB = 'TA' OR TRIM(a.DEAFTB) = '' THEN NULL ELSE CASE WHEN a.DEAREY=0 AND a.DEARCM=0 AND a.DEARCO=0 THEN CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.DEAODY 
                THEN CONCAT('19', LPAD(CAST(a.DEAODY AS string), 2, '0')) 
                ELSE CONCAT('2', LPAD(CAST(a.DEAODY AS string), 3, '0')) END, 
        LPAD(CAST(a.DEAODM AS string), 2, '0'), LPAD(CAST(a.DEAODD  AS string), 2, '0'))
        ELSE CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.DEAREY 
                         THEN CONCAT('19', LPAD(CAST(a.DEAREY AS string), 2, '0')) 
                         ELSE CONCAT('2', LPAD(CAST(a.DEAREY AS string), 3, '0')) END, 
            LPAD(CAST(a.DEARCM AS string), 2, '0'), LPAD(CAST(a.DEARCO  AS string), 2, '0'))
        END END AS DATE_LAST_REV, 
    CASE WHEN a.DEAFTB = 'TA' OR TRIM(a.DEAFTB) = '' THEN NULL ELSE CASE WHEN a.DEARDY=0 AND a.DEARDM=0 AND a.DEARDD=0 THEN NULL
        ELSE CONCAT(CONCAT('2', LPAD(CAST(a.DEARDY AS string), 3, '0')), 
                    LPAD(CAST(a.DEARDM AS string), 2, '0'), LPAD(CAST(a.DEARDD  AS string), 2, '0'))
        END END AS DATE_PRX_REV,
    CASE WHEN a.DEAMAY=0 AND a.DEAMAM=0 AND a.DEAMAD=0 THEN NULL
        ELSE CONCAT(CONCAT('2', LPAD(CAST(a.DEAMAY AS string), 3, '0')), 
                    LPAD(CAST(a.DEAMAM AS string), 2, '0'), LPAD(CAST(a.DEAMAD  AS string), 2, '0')) 
        END AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, 
    CAST(CASE WHEN a.DEARRY=0 AND a.DEARRM=0 AND a.DEARRD=0 THEN NULL
        ELSE CONCAT(CONCAT('2', LPAD(CAST(a.DEARRY AS string), 3, '0')), 
                    LPAD(CAST(a.DEARRM AS string), 2, '0'), LPAD(CAST(a.DEARRD  AS string), 2, '0')) 
        END AS bigint) AS FRE_REV_INT, 
    a.DEARRP AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, a.DEAOAM AS INI_AM, NULL AS CUO_AM, 
    NULL AS CREDIT_LIM_AM, NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, 
    TRIM(a.DEASCH) AS IND_CHANNEL, 
    CASE WHEN  STRLEFT(TRIM(A.DEADST),2) = 'PA' THEN 'LOCAL' ELSE 'INTERNACIONAL' END COD_TYP_LIC,
    TRIM(a.DEASST) AS COD_SELLER, TRIM(c.CNODSC) AS DES_SELLER, NULL AS COU_CAR_OFF, NULL AS COD_CONV, a.DEASST AS COD_EXEC_CTO, CASE WHEN cov.IDF_CTO IS NULL THEN 'N' ELSE 'Y' END AS COD_COVID_PORT,     CONCAT(CASE WHEN CAST(STRRIGHT(CAST(YEAR(NOW()) AS string), 2) AS smallint) < a.DEAODY 
                THEN CONCAT('19', LPAD(CAST(a.DEAODY AS string), 2, '0')) 
                ELSE CONCAT('2', LPAD(CAST(a.DEAODY AS string), 3, '0')) END, 
        LPAD(CAST(a.DEAODM AS string), 2, '0'), LPAD(CAST(a.DEAODD  AS string), 2, '0')) AS DATE_DISB, NULL AS CARD_NUMBER, NULL AS COD_PROG_CARD, NULL AS DES_PROG_CARD
FROM MIS_LOAD_DEALS a
LEFT JOIN MIS_PAR_REL_PROD_SPE b 
ON CAST(a.DEAACC AS string) = B.IDF_CTO 
LEFT JOIN (SELECT CNOCFL, CNORCD, MAX(CNODSC) AS CNODSC FROM MIS_LOAD_CNOFC WHERE TRIM(CNOCFL) IN ('65') GROUP BY CNORCD, CNOCFL) c
ON TRIM(a.DEASST) = TRIM(c.CNORCD)
/*LEFT JOIN (SELECT CNOCFL, CNORCD, MAX(CNODSC) AS CNODSC FROM MIS_LOAD_CNOFC WHERE TRIM(CNOCFL) IN ('62') GROUP BY CNORCD, CNOCFL) cha
ON TRIM(a.DEASCH) = TRIM(cha.CNORCD)*/
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.DEACUN AS STRING)
LEFT JOIN (SELECT * FROM MIS_PAR_REL_CAR_COVID WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6)) cov
ON CAST(a.DEAACC AS string) = cov.IDF_CTO
WHERE a.DEABNK <> '02' AND a.DEASTS <> 'C' AND CAST(a.DEAACC AS STRING) <> '0';

--COMPUTE STATS MIS_LOAD_S3STRA;
--COMPUTE STATS MIS_LOAD_SGWTRA;

ALTER TABLE MIS_APR_S3STRA
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_APR_S3STRA
PARTITION (DATA_DATE='${var:periodo}')
select s3snum, s3ssal, s3smon, s3slim, s3ssa1, s3sfec, s3sfe1, s3spag, s3smo1, s3stas, s3ssa2, s3snu1, s3sfe2, s3sfe3, s3sest, s3ses1, s3semi, s3scue, s3sfe4, s3sdir, s3sdi1, s3sdi2,  
s3slti, s3slt1, s3slt2, s3slt3, s3stip, s3sli1, s3sta1, s3star, s3sfe5, case when date_cap is null or trim(date_cap) = '' then s3sfe5 else date_cap end as date_cap
from 
(select * from MIS_LOAD_S3STRA where TO_TIMESTAMP(TRIM(S3SFE5), 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1)) a
left join (select * from mis_par_cierres_tar where strleft(date_start,6) = strleft(from_timestamp(DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1),'yyyyMMdd'),6)) b
on '${var:periodo}' = date_start
;

ALTER TABLE MIS_APR_SGWTRA
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_APR_SGWTRA
PARTITION (DATA_DATE='${var:periodo}')
select sgwcod, sgwnum, sgwced, sgwcic, sgwfec, sgwce1, sgwfe1, sgwco1, sgwest, sgwfe2, sgwlim, sgwcom, sgwfe3, sgwfe4, sgwfe5, sgwact,
sgwfe6, sgwdir, sgwdi1, sgwciu, sgwes1, sgwco2, sgwapa, sgwind, sgwnu1, sgwhor, sgwcla, sgwcl1, sgwcon, sgwmod, sgwapl, sgwult, sgwul1, sgwul2, sgwul3, sgwul4, sgwul5, sgwul6, sgwsal, sgwsa1, sgwfe7, sgwfe8, sgwmon, sgwmo1, sgwcre, sgwcr1, sgwcr2, sgwcr3, sgwcta, sgwofi, sgwdes, sgwpag, sgwpa1, sgwnu3, sgwta1, sgwtas, sgwnu2, sgwnom, sgwema, sgwta2, sgwta3, sgwsa2, sgwsa3, sgwsa4, sgwsa5, sgwsa6, sgwsa7, sgwsa8, sgwsa9, sgws01, sgws02, sgws03, sgws04, sgwcan, sgwca1, sgwcap, sgwca2, sgwint, sgwin1, sgwmor, sgwmo2, sgwotr, sgwot1, sgwc03, sgwca3, sgwca4, sgwin2, sgwin3, sgwdat, case when date_cap is null or trim(date_cap) = '' then sgwdat else date_cap end as date_cap
from 
(select * from MIS_LOAD_SGWTRA where TO_TIMESTAMP(TRIM(sgwdat), 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 1)) a
left join (select * from mis_par_cierres_tar where strleft(date_start,6) = strleft(from_timestamp(DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1),'yyyyMMdd'),6)) b
on '${var:periodo}' = date_start
;

----Aprovisionamiento de Tarjetas
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, 
    '0' AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, TRIM(a.S3SMON) AS COD_CURRENCY, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE,  
    CAST(IF(IFNULL(a.S3SSA1, 0) + IFNULL(b.SGWSA5, 0) < 0, 0, IFNULL(a.S3SSA1, 0)) AS decimal(30, 10)) AS EOPBAL_CAP,  
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'S3STRA' AS COD_INFO_SOURCE 
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_SGWTRA b
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(b.SGWCOD), 2), TRIM(b.SGWNUM)) 
    AND TO_TIMESTAMP(TRIM(b.DATA_DATE), 'yyyyMMdd') = TO_TIMESTAMP(TRIM(a.DATA_DATE), 'yyyyMMdd')
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string) 
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}')
;

----Aprovisionamiento de Tarjetas intra
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE), 'I') AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, 
    '0' AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, TRIM(a.S3SMON) AS COD_CURRENCY, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CONCAT(CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END,'_INT') AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE,
    CAST(IF(IFNULL(a.S3SSA1, 0) + IFNULL(b.SGWSA5, 0) < 0, 0, IFNULL(b.SGWSA5, 0)) AS decimal(30, 10)) AS EOPBAL_CAP,
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'S3STRA' AS COD_INFO_SOURCE 
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_SGWTRA b
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(b.SGWCOD), 2), TRIM(b.SGWNUM)) 
    AND TO_TIMESTAMP(TRIM(b.DATA_DATE), 'yyyyMMdd') = TO_TIMESTAMP(TRIM(a.DATA_DATE), 'yyyyMMdd')
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string) 
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}')
;

----Aprovisionamiento de Tarjetas extra
INSERT INTO MIS_APR_ASSETS 
PARTITION (DATA_DATE='${var:periodo}') 
SELECT 'PREST' AS COD_CONT, CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE), 'E') AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, 
    '0' AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'CAP' AS COD_VALUE, TRIM(a.S3SMON) AS COD_CURRENCY, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CONCAT(CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END,'_EXT') AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE,  
    CAST(IFNULL(b.SGWSA9, 0) AS decimal(30, 10)) AS EOPBAL_CAP,  
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, NULL AS PL, 'S3STRA' AS COD_INFO_SOURCE 
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_SGWTRA b
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(b.SGWCOD), 2), TRIM(b.SGWNUM)) 
    AND TO_TIMESTAMP(TRIM(b.DATA_DATE), 'yyyyMMdd') = TO_TIMESTAMP(TRIM(a.DATA_DATE), 'yyyyMMdd')
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string) 
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}')
;


ALTER TABLE MIS_APR_CONTRACT_DT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_INFO_SOURCE='S3STRA');

----Aprovisionamiento de Contratos asociados a Tarjetas
INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='S3STRA')
SELECT CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) AS IDF_CTO, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END AS COD_SUBPRODUCT,  '' AS COD_ACT_TYPE, 
    TRIM(a.S3SMON) AS COD_CURRENCY, TRIM(a.S3SNU1) AS IDF_CLI, NULL AS COD_ACCO_CENT, '0' AS COD_OFFI, 
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, CAST(a.S3STA1/100 AS decimal(30, 10)) AS RATE_INT, 
    TRIM(a.S3SFE2) AS DATE_ORIGIN, NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, TRIM(a.S3SFE3) AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, a.S3SLIM AS INI_AM, a.S3SMO1 AS CUO_AM, a.S3SLIM AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, 'INTERNACIONAL' AS COD_TYP_LIC, NULL AS COD_SELLER, NULL AS DES_SELLER, NULL AS COU_CAR_OFF, NULL AS COD_CONV, NULL AS COD_EXEC_CTO, CASE WHEN cov.IDF_CTO IS NULL THEN 'N' ELSE 'Y' END AS COD_COVID_PORT, TRIM(a.S3SFE2) AS DATE_DISB, a.S3SNUM AS CARD_NUMBER, le.S3NPLA AS COD_PROG_CARD, pale.DES_PROG_CARD
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string)
LEFT JOIN (SELECT * FROM MIS_PAR_REL_CAR_COVID WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6)) cov
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = cov.IDF_CTO
LEFT JOIN (SELECT S3NEMI, S3NCUE, MAX(S3NPLA) AS S3NPLA FROM MIS_LOAD_S3NTRA
WHERE DATA_DATE = '${var:periodo}'
GROUP BY S3NEMI, S3NCUE) le
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(le.S3NEMI), 2), TRIM(le.S3NCUE))
LEFT JOIN MIS_PAR_REL_PROG_CARD pale
ON le.S3NPLA = pale.COD_PROG_CARD
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}')
;


INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='S3STRA')
SELECT CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE), 'I') AS IDF_CTO, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END AS COD_SUBPRODUCT,  '' AS COD_ACT_TYPE, 
    TRIM(a.S3SMON) AS COD_CURRENCY, TRIM(a.S3SNU1) AS IDF_CLI, NULL AS COD_ACCO_CENT, '0' AS COD_OFFI, 
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, CAST(a.S3STA1/100 AS decimal(30, 10)) AS RATE_INT, 
    TRIM(a.S3SFE2) AS DATE_ORIGIN, NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, TRIM(a.S3SFE3) AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, a.S3SLIM AS INI_AM, a.S3SMO1 AS CUO_AM, a.S3SLIM AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, 'INTERNACIONAL' AS COD_TYP_LIC, NULL AS COD_SELLER, NULL AS DES_SELLER, NULL AS COU_CAR_OFF, NULL AS COD_CONV, NULL AS COD_EXEC_CTO, CASE WHEN cov.IDF_CTO IS NULL THEN 'N' ELSE 'Y' END AS COD_COVID_PORT, TRIM(a.S3SFE2) AS DATE_DISB, a.S3SNUM AS CARD_NUMBER, le.S3NPLA AS COD_PROG_CARD, pale.DES_PROG_CARD
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_SGWTRA b
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(b.SGWCOD), 2), TRIM(b.SGWNUM)) 
    AND TO_TIMESTAMP(TRIM(b.DATA_DATE), 'yyyyMMdd') = TO_TIMESTAMP(TRIM(a.DATA_DATE), 'yyyyMMdd')
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string)
LEFT JOIN (SELECT * FROM MIS_PAR_REL_CAR_COVID WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6)) cov
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = cov.IDF_CTO
LEFT JOIN (SELECT S3NEMI, S3NCUE, MAX(S3NPLA) AS S3NPLA FROM MIS_LOAD_S3NTRA
WHERE DATA_DATE = '${var:periodo}'
GROUP BY S3NEMI, S3NCUE) le
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(le.S3NEMI), 2), TRIM(le.S3NCUE))
LEFT JOIN MIS_PAR_REL_PROG_CARD pale
ON le.S3NPLA = pale.COD_PROG_CARD
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}') AND b.SGWSA5 <> 0
;


INSERT INTO MIS_APR_CONTRACT_DT 
PARTITION (DATA_DATE='${var:periodo}', COD_INFO_SOURCE='S3STRA')
SELECT CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE), 'E') AS IDF_CTO, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT,
    CASE STRLEFT(TRIM(a.S3SNUM), 4) 
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END AS COD_SUBPRODUCT,  '' AS COD_ACT_TYPE, 
    TRIM(a.S3SMON) AS COD_CURRENCY, TRIM(a.S3SNU1) AS IDF_CLI, NULL AS COD_ACCO_CENT, '0' AS COD_OFFI, 
    NULL AS COD_BCA_INT, NULL AS COD_AMRT_MET, 'F' AS COD_RATE_TYPE, CAST(a.S3STA1/100 AS decimal(30, 10)) AS RATE_INT, 
    TRIM(a.S3SFE2) AS DATE_ORIGIN, NULL AS DATE_LAST_REV, NULL AS DATE_PRX_REV, TRIM(a.S3SFE3) AS EXP_DATE, 
    NULL AS FREQ_INT_PAY, NULL AS COD_UNI_FREQ_INT_PAY, NULL AS FRE_REV_INT, NULL AS COD_UNI_FRE_REV_INT, 
    NULL AS AMRT_TRM, NULL AS COD_UNI_AMRT_TRM, a.S3SLIM AS INI_AM, a.S3SMO1 AS CUO_AM, a.S3SLIM AS CREDIT_LIM_AM, 
    NULL AS PREDEF_RATE_IND, NULL AS PREDEF_RATE, NULL AS IND_CHANNEL, 'INTERNACIONAL' AS COD_TYP_LIC, NULL AS COD_SELLER, NULL AS DES_SELLER, NULL AS COU_CAR_OFF, NULL AS COD_CONV, NULL AS COD_EXEC_CTO, CASE WHEN cov.IDF_CTO IS NULL THEN 'N' ELSE 'Y' END AS COD_COVID_PORT, TRIM(a.S3SFE2) AS DATE_DISB, a.S3SNUM AS CARD_NUMBER, le.S3NPLA AS COD_PROG_CARD, pale.DES_PROG_CARD
FROM MIS_APR_S3STRA a
LEFT JOIN MIS_APR_SGWTRA b
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(b.SGWCOD), 2), TRIM(b.SGWNUM)) 
    AND TO_TIMESTAMP(TRIM(b.DATA_DATE), 'yyyyMMdd') = TO_TIMESTAMP(TRIM(a.DATA_DATE), 'yyyyMMdd')
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.S3SNU1 AS string)
LEFT JOIN (SELECT * FROM MIS_PAR_REL_CAR_COVID WHERE STRLEFT(DATA_DATE,6) = STRLEFT('${var:periodo}',6)) cov
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = cov.IDF_CTO
LEFT JOIN (SELECT S3NEMI, S3NCUE, MAX(S3NPLA) AS S3NPLA FROM MIS_LOAD_S3NTRA
WHERE DATA_DATE = '${var:periodo}'
GROUP BY S3NEMI, S3NCUE) le
ON CONCAT(SUBSTR(TRIM(a.S3SEMI), 2), TRIM(a.S3SCUE)) = CONCAT(SUBSTR(TRIM(le.S3NEMI), 2), TRIM(le.S3NCUE))
LEFT JOIN MIS_PAR_REL_PROG_CARD pale
ON le.S3NPLA = pale.COD_PROG_CARD
WHERE TRIM(a.S3STIP) = 'T' AND a.s3sfe5 in (select distinct date_cap from mis_apr_s3stra where data_date='${var:periodo}') AND b.SGWSA9 <> 0
;