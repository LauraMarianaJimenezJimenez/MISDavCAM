--------------------------------------------------------------- MIS_APR_FEES --------------------------------------------------------------

USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load
TRUNCATE TABLE IF EXISTS MIS_LOAD_MOV;
LOAD DATA INPATH '${var:ruta_fuentes_comisiones}/MOV.CSV' INTO TABLE MIS_LOAD_MOV;

TRUNCATE TABLE IF EXISTS MIS_LOAD_DCMST;
LOAD DATA INPATH '${var:ruta_fuentes_comisiones}/DCMST.CSV' INTO TABLE MIS_LOAD_DCMST;

/*
DROP TABLE IF EXISTS MIS_LOAD_MOV_TMP;
CREATE TABLE MIS_LOAD_MOV_TMP AS
select t40cod, t40f01, t40doc, t40do1, t40est, t40co1, t40nu1, t40num, t40fec, case when date_pyg is null then t40fe1 else date_pyg end as t40fe1, t40hor, t40tas, t40usu, t40co2, t40nu2, t40mon, t40co4, t40co3, t40co5, t40nu5, t40ter, t40nu4, t40dat
from (select * from MIS_LOAD_MOV where TO_TIMESTAMP(t40fe1, 'yyyyMMdd') BETWEEN DAYS_SUB(TRUNC(DAYS_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),'MM'),1),'MM'),1)
AND TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') AND MONTH(TO_TIMESTAMP(t40fe1, 'yyyyMMdd')) = MONTH(DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1))) a
left join (select * from mis_par_cierres_tar where strleft(date_start,6) = strleft('${var:periodo}',6)) b
on t40fe1 = date_start
;
*/

ALTER TABLE MIS_APR_MOV
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO MIS_APR_MOV
PARTITION (DATA_DATE='${var:periodo}')
select t40cod, t40f01, t40doc, t40do1, t40est, t40co1, t40nu1, t40num, t40fec, case when date_pyg is null then '${var:periodo}' else date_pyg end as t40fe1, t40hor, t40tas, t40usu, t40co2, 
t40nu2, t40mon, t40co4, t40co3, t40co5, t40nu5, t40ter, t40nu4, t40dat
from (select * from MIS_LOAD_MOV where T40EST NOT IN ('N') and TO_TIMESTAMP(t40fe1, 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1) AND MONTH(TO_TIMESTAMP(t40fe1, 'yyyyMMdd')) = MONTH(DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1))) a
left join (select * from mis_par_cierres_tar where strleft(date_start,6) = strleft(from_timestamp(DATE_SUB(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1),'yyyyMMdd'),6)) b
on t40fe1 = date_start
;

--Limpieza de partición del día en ejecución
ALTER TABLE MIS_APR_FEES
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

COMPUTE INCREMENTAL STATS MIS_APR_ASSETS partition (data_date = '${var:periodo}');

----Aprovisionamiento de Comisiones ganadas y Gastos comisiones
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
    --Comisiones registradas en el dia de ejecucion
    SELECT 'COM' AS COD_CONT, CONCAT(SUBSTR(TRIM(a.T40CO1), 2), TRIM(a.T40NU1)) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, NULL AS COD_OFFI, '0' AS COD_BLCE_STATUS, 
        CASE WHEN TRIM(a.T40COD) IN ('D8') THEN 'COM_GSTO'
             WHEN TRIM(a.T40COD) IN ('13', '15', '29', '35', '53', '55', '56', '90', '5Q', 'C0', 'FJ', 
                                     'D8','DG','65','66','67','68','69','6E','EG','JF','JT','RH','CU') THEN 'COM_GNDA'
             ELSE 'N/A' END AS COD_VALUE,
        'USD' AS COD_CURRENCY, '01' AS COD_ENTITY, 
        CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
        CASE WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5480' THEN 'MCG'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5209' THEN 'MCP'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5223' THEN 'MCS'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5529' THEN 'MCB'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5102' THEN 'MCK'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5164' THEN 'MCD'
        END AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
    CASE WHEN TRIM(a.T40COD) IN ('65','66','67','68','69','6E','EG','JF','JT','RH','DG') THEN CAST(SUM(a.T40MON) AS decimal(30, 10)) ELSE CAST(SUM(a.T40MON * -1) AS decimal(30, 10)) END AS PL, 'MOV' AS COD_INFO_SOURCE
    FROM MIS_APR_MOV a 
    LEFT JOIN MIS_APR_CLIENT_DT cl 
    ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.T40NU1 AS string) 
    WHERE TRIM(a.T40COD) IN ('13', '15', '29', '35', '53', '55', '56', '90', '5Q', 'C0', 'FJ', 'D8','DG','65','66','67','68','69','6E','EG','JF','JT','RH','CU')
            AND TO_TIMESTAMP(a.t40fe1, 'yyyyMMdd') BETWEEN SUBDATE(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), DAYOFMONTH(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) - 1) AND TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')
GROUP BY a.T40CO1, a.T40NU1, a.T40COD, cl.COD_SECTOR, a.T40NUM; 


INSERT INTO MIS_APR_ASSETS
PARTITION (DATA_DATE='${var:periodo}')
    --Intereses por tarjeta registrados en el dia de ejecucion
    SELECT 'PREST' AS COD_CONT, CONCAT(SUBSTR(TRIM(a.T40CO1), 2), TRIM(a.T40NU1)) AS IDF_CTO, NULL AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, 
        NULL AS COD_OFFI,  
        CASE WHEN TRIM(a.T40COD) IN ('CW') THEN '2'
             WHEN TRIM(a.T40COD) IN ('11', '88', 'IP', '6V', 'RI') THEN '1'
             ELSE 'N/A' END AS COD_BLCE_STATUS, 
        'RSL' AS COD_VALUE, 'USD' AS COD_CURRENCY, '01' AS COD_ENTITY, 
        CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
        CASE WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5480' THEN 'MCG'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5209' THEN 'MCP'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5223' THEN 'MCS'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5529' THEN 'MCB'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5102' THEN 'MCK'
            WHEN STRLEFT(TRIM(a.T40NUM), 4) = '5164' THEN 'MCD'
        END AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT,
        CAST(SUM(CASE WHEN TRIM(a.T40COD) IN ('6V', 'RI') THEN a.T40MON ELSE a.T40MON * -1 END) AS decimal(30, 10)) AS PL, 
        'MOV' AS COD_INFO_SOURCE
    FROM MIS_APR_MOV a 
    LEFT JOIN MIS_APR_CLIENT_DT cl 
    ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(a.T40NU1 AS string) 
    WHERE TRIM(a.T40COD) IN ('11', '88', 'IP', 'CW', '6V', 'RI') 
            AND TO_TIMESTAMP(a.t40fe1, 'yyyyMMdd') BETWEEN SUBDATE(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), DAYOFMONTH(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) - 1) AND TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')
GROUP BY a.T40CO1, a.T40NU1, a.T40COD, cl.COD_SECTOR, a.T40NUM; 


----Aprovisionamiento de Comisiones TRANS
--- Cruce contra deals--
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
SELECT COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(SUM(PL) AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE
FROM(
SELECT 'COM' AS COD_CONT, CASE WHEN b.DEAACC IS NOT NULL THEN CAST(a.traacc AS string) ELSE CAST(a.TRAACR AS string) END AS IDF_CTO, tragln AS COD_GL, NULL AS DES_GL, CAST(ISNULL(b.DEACCN,z.DEACCN) AS string) AS COD_ACCO_CENT, 
    TRIM(ISNULL(b.DEABRN,z.DEABRN)) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, ISNULL(b.DEACCY,z.DEACCY) AS COD_CURRENCY, ISNULL(b.DEABNK,z.DEABNK) AS COD_ENTITY,
    CONCAT(
        CASE WHEN d.IDF_CTO IS NOT NULL THEN d.COD_PRODUCT
            WHEN ISNULL(b.DEAICD,z.DEAICD) = 2101 THEN 'VIV'
            ELSE TRIM(ISNULL(b.DEATYP,z.DEATYP)) END,
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(ISNULL(b.DEAPRO,z.DEAPRO)) AS COD_SUBPRODUCT, IFNULL(CAST(ISNULL(b.DEAICD,z.DEAICD) AS STRING), '') AS COD_ACT_TYPE, NULL AS EOPBAL_CAP, 
    NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CASE WHEN tradcc = '5' THEN CAST(TRAAMT * -1 AS DECIMAL(30,10)) ELSE TRAAMT END AS PL, 'TRANS' AS COD_INFO_SOURCE
FROM mis_load_trans a
LEFT JOIN mis_load_deals b
on a.traacc = b.deaacc
LEFT JOIN mis_load_deals z
on a.traacr = z.deaacc
INNER JOIN (select * from mis_par_rel_pl_acc 
where cod_pl_acc in(select distinct cod_level_12 from mis_hierarchy_pl_acc where cod_level_02 = 'SER_NET')) c
on tragln = c.cod_gl_group and trabnk= c.cod_entity and trim(a.traccy) = c.cod_currency
LEFT JOIN MIS_PAR_REL_PROD_SPE d
ON CAST(b.DEAACC AS string) = d.IDF_CTO
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(ISNULL(b.DEACUN,z.DEACUN) AS string)
where (b.deaacc is not null or z.deaacc is not null) and
cast(trabdy as string) = substr('${var:periodo}',3,2)
and lpad(cast(trabdm as string),2,'0') = substr('${var:periodo}',5,2)
)A
GROUP BY COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, COD_INFO_SOURCE
;

--- Cruce contra acmst---
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
SELECT COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(SUM(PL) AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE
FROM(
SELECT 'COM' AS COD_CONT, TRIM(ISNULL(b.ACMACC,z.ACMACC)) AS IDF_CTO, tragln AS COD_GL, NULL AS DES_GL, TRIM(ISNULL(b.ACMCCN,z.ACMCCN)) AS COD_ACCO_CENT, TRIM(ISNULL(b.ACMBRN,z.ACMBRN)) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 
    'RSL' AS COD_VALUE, TRIM(ISNULL(b.ACMCCY,z.ACMCCY)) AS COD_CURRENCY, TRIM(ISNULL(b.ACMBNK,z.ACMBNK)) AS COD_ENTITY, 
    CONCAT(TRIM(ISNULL(b.ACMATY,z.ACMATY)), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(ISNULL(b.ACMPRO,z.ACMPRO)) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CASE WHEN tradcc = '5' THEN CAST(traamt * -1 AS DECIMAL(30,10)) ELSE traamt END AS PL, 'TRANS' AS COD_INFO_SOURCE
FROM mis_load_trans a
LEFT JOIN mis_load_acmst b
on cast(a.traacc as string) = cast(b.acmacc as string)
LEFT JOIN mis_load_acmst z
on cast(a.traacr as string) = cast(z.acmacc as string)
INNER JOIN (select * from mis_par_rel_pl_acc 
where cod_pl_acc in(select distinct cod_level_12 from mis_hierarchy_pl_acc where cod_level_02 = 'SER_NET')) c
on tragln = c.cod_gl_group and trabnk= c.cod_entity and trim(a.traccy) = c.cod_currency
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(ISNULL(b.ACMCUN,z.ACMCUN) AS string)
where (b.acmacc is not null or z.acmacc is not null) and
cast(trabdy as string) = substr('${var:periodo}',3,2)
and lpad(cast(trabdm as string),2,'0') = substr('${var:periodo}',5,2)
)A
GROUP BY COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, COD_INFO_SOURCE
;

--- Cruce contra lcmst---
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
SELECT COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(SUM(PL) AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE
FROM(
SELECT 'COM' AS COD_CONT, ISNULL(b.LCMACC,z.LCMACC) AS IDF_CTO, tragln AS COD_GL, NULL AS DES_GL, CAST(ISNULL(b.LCMCCN,z.LCMCCN) AS string) AS COD_ACCO_CENT, 
    ISNULL(b.LCMBRN,z.LCMBRN) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, ISNULL(b.LCMCCY,z.LCMCCY) AS COD_CURRENCY, ISNULL(b.LCMBNK,z.LCMBNK) AS COD_ENTITY, 
    CONCAT(TRIM(ISNULL(b.LCMATY,z.LCMATY)),
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(ISNULL(b.LCMPRO,z.LCMPRO)) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CASE WHEN tradcc = '5' THEN CAST(traamt * -1 AS DECIMAL(30,10)) ELSE traamt END AS PL, 'TRANS' AS COD_INFO_SOURCE
FROM mis_load_trans a
LEFT JOIN mis_load_lcmst b
on cast(a.traacc as string) = cast(b.lcmacc as string)
LEFT JOIN mis_load_lcmst z
on cast(a.traacr as string) = cast(z.lcmacc as string)
INNER JOIN (select * from mis_par_rel_pl_acc 
where cod_pl_acc in(select distinct cod_level_12 from mis_hierarchy_pl_acc where cod_level_02 = 'SER_NET')) c
on tragln = c.cod_gl_group and trabnk= c.cod_entity and trim(a.traccy) = c.cod_currency
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(ISNULL(b.LCMCUN,z.LCMCUN) AS string)
where (b.lcmacc is not null or z.lcmacc is not null) and
cast(trabdy as string) = substr('${var:periodo}',3,2)
and lpad(cast(trabdm as string),2,'0') = substr('${var:periodo}',5,2)
)A
GROUP BY COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, COD_INFO_SOURCE
;

--- Cruce contra s3stra---
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
SELECT COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(SUM(PL) AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE
FROM(
SELECT 'COM' AS COD_CONT, ISNULL(CONCAT(SUBSTR(TRIM(b.S3SEMI), 2), TRIM(b.S3SCUE)),CONCAT(SUBSTR(TRIM(z.S3SEMI), 2), TRIM(z.S3SCUE))) AS IDF_CTO, 
    TRAGLN AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT, '0' AS COD_OFFI, '1' AS COD_BLCE_STATUS, 'RSL' AS COD_VALUE, 
    ISNULL(b.S3SMON,z.S3SMON) AS COD_CURRENCY, '01' AS COD_ENTITY, 
    CONCAT('M/C', 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    CASE STRLEFT(TRIM(ISNULL(b.S3SNUM,z.S3SNUM)), 4)
        WHEN '5480' THEN 'MCG'
        WHEN '5209' THEN 'MCP'
        WHEN '5223' THEN 'MCS'
        WHEN '5529' THEN 'MCB'
        WHEN '5102' THEN 'MCK'
        WHEN '5164' THEN 'MCD'
    END AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, 
    CASE WHEN tradcc = '5' THEN CAST(traamt * -1 AS DECIMAL(30,10)) ELSE traamt END AS PL, 'TRANS' AS COD_INFO_SOURCE
FROM mis_load_trans a
LEFT JOIN (SELECT * FROM MIS_LOAD_S3STRA
WHERE TO_TIMESTAMP(TRIM(S3SFE5), 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('20211231', 'yyyyMMdd'), 1)) b
on cast(a.TRAACC as string) = CONCAT(SUBSTR(TRIM(b.S3SEMI), 2), TRIM(b.S3SCUE))
LEFT JOIN (SELECT * FROM MIS_LOAD_S3STRA
WHERE TO_TIMESTAMP(TRIM(S3SFE5), 'yyyyMMdd') = DATE_SUB(TO_TIMESTAMP('20211231', 'yyyyMMdd'), 1)) z
on cast(a.TRAACR as string) = CONCAT(SUBSTR(TRIM(z.S3SEMI), 2), TRIM(z.S3SCUE))
INNER JOIN (select * from mis_par_rel_pl_acc 
where cod_pl_acc in(select distinct cod_level_12 from mis_hierarchy_pl_acc where cod_level_02 = 'SER_NET')) c
on tragln = c.cod_gl_group and trabnk= c.cod_entity and trim(a.traccy) = c.cod_currency
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '20211231' AND cl.IDF_CLI = CAST(ISNULL(b.S3SNU1,z.S3SNU1) AS string)
where (b.S3SCUE IS NOT NULL OR z.S3SCUE IS NOT NULL) and
cast(trabdy as string) = substr('20211231',3,2)
and lpad(cast(trabdm as string),2,'0') = substr('20211231',5,2)
)A
GROUP BY COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, COD_INFO_SOURCE
;

--- Cruce contra dcmst---
INSERT INTO MIS_APR_FEES
PARTITION (DATA_DATE='${var:periodo}')
SELECT COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(SUM(PL) AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE
FROM(
SELECT 'COM' AS COD_CONT, TRIM(ISNULL(b.DCMACC,z.DCMACC)) AS IDF_CTO, tragln AS COD_GL, NULL AS DES_GL, TRIM(ISNULL(b.DCMCCN,z.DCMCCN)) AS COD_ACCO_CENT, TRIM(ISNULL(b.DCMBRN,z.DCMBRN)) AS COD_OFFI, '1' AS COD_BLCE_STATUS, 
    'RSL' AS COD_VALUE, TRIM(ISNULL(b.DCMCCY,z.DCMCCY)) AS COD_CURRENCY, TRIM(ISNULL(b.DCMBNK,z.DCMBNK)) AS COD_ENTITY, 
    CONCAT(TRIM(ISNULL(b.DCMATY,z.DCMATY)), 
        CASE cl.COD_SECTOR
            WHEN 'INTERNACIONAL' THEN '_I'
            WHEN 'LOCAL' THEN '_L'
            ELSE '_L' END) AS COD_PRODUCT, 
    TRIM(ISNULL(b.DCMPRO,z.DCMPRO)) AS COD_SUBPRODUCT, '' AS COD_ACT_TYPE, 
    NULL AS EOPBAL_CAP, NULL AS EOPBAL_INT, 
    NULL AS AVGBAL_CAP, NULL AS AVGBAL_INT, CASE WHEN tradcc = '5' THEN CAST(traamt * -1 AS DECIMAL(30,10)) ELSE traamt END AS PL, 'TRANS' AS COD_INFO_SOURCE
FROM mis_load_trans a
LEFT JOIN mis_load_dcmst b
on cast(a.traacc as string) = cast(b.dcmacc as string)
LEFT JOIN mis_load_dcmst z
on cast(a.traacr as string) = cast(z.dcmacc as string)
INNER JOIN (select * from mis_par_rel_pl_acc 
where cod_pl_acc in(select distinct cod_level_12 from mis_hierarchy_pl_acc where cod_level_02 = 'SER_NET')) c
on tragln = c.cod_gl_group and trabnk= c.cod_entity and trim(a.traccy) = c.cod_currency
LEFT JOIN MIS_APR_CLIENT_DT cl 
ON cl.DATA_DATE = '${var:periodo}' AND cl.IDF_CLI = CAST(ISNULL(b.DCMCUN,z.DCMCUN) AS string)
where (b.dcmacc is not null or z.dcmacc is not null) and
cast(trabdy as string) = substr('${var:periodo}',3,2)
and lpad(cast(trabdm as string),2,'0') = substr('${var:periodo}',5,2)
)A
GROUP BY COD_CONT, IDF_CTO, COD_GL, DES_GL, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_VALUE, COD_CURRENCY, COD_ENTITY, COD_PRODUCT,
COD_SUBPRODUCT, COD_ACT_TYPE, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, COD_INFO_SOURCE
;