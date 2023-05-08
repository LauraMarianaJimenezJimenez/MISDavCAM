--------------------------------------------------------------- MIS_APR_ACCOUNTING --------------------------------------------------

--- Seleccion de Base de datos de Ejecución ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Eliminacion y Carga de la tabla GLP003 ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_GLP00301_MP; 
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/GLP00301.csv' INTO TABLE MIS_LOAD_GLP00301_MP;

--- Eliminacion y Carga de la tabla GLP013 ---
TRUNCATE TABLE IF EXISTS MIS_LOAD_GLP01301_MP;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/GLP01301.csv' INTO TABLE MIS_LOAD_GLP01301_MP;

--- Limpieza de partición ----
ALTER TABLE MIS_APR_ACCOUNTING
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');


--- Aprovisionamiento de información ---
INSERT INTO MIS_APR_ACCOUNTING
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'CONT' AS COD_CONT, 
        RPAD(TRIM(A.GMACT),16,'0') AS COD_GL, 
        A.GMNAME AS DES_GL,
        A.GMCNTR AS COD_ACCO_CENT,
        NULL AS COD_OFFI,
        NULL AS COD_NAR,
        A.GMSTAT AS COD_BLCE_STATUS,
        CASE 
             WHEN A.GMCURC IN('1','2') THEN 'USD' 
             WHEN A.GMCURC IN('3','4') THEN 'EUR'
        ELSE 'HNL' END AS COD_CURRENCY,
--        'HNL' AS COD_CURRENCY,
        A.GMBK AS COD_ENTITY,
        CASE WHEN strleft(A.GMACT,1) NOT IN ('5','6') THEN CASE MONTH(CAST(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') AS TIMESTAMP)) 
             WHEN 1 THEN CAST(B.GMCH01 AS DECIMAL(30,10))
             WHEN 2 THEN CAST(B.GMCH02 AS DECIMAL(30,10))
             WHEN 3 THEN CAST(B.GMCH03 AS DECIMAL(30,10))
             WHEN 4 THEN CAST(B.GMCH04 AS DECIMAL(30,10))
             WHEN 5 THEN CAST(B.GMCH05 AS DECIMAL(30,10))
             WHEN 6 THEN CAST(B.GMCH06 AS DECIMAL(30,10))
             WHEN 7 THEN CAST(B.GMCH07 AS DECIMAL(30,10))
             WHEN 8 THEN CAST(B.GMCH08 AS DECIMAL(30,10))
             WHEN 9 THEN CAST(B.GMCH09 AS DECIMAL(30,10))
             WHEN 10 THEN CAST(B.GMCH10 AS DECIMAL(30,10))
             WHEN 11 THEN CAST(B.GMCH11 AS DECIMAL(30,10))
             WHEN 12 THEN CAST(B.GMCH12 AS DECIMAL(30,10))
        ELSE 0
        END ELSE 0 END AS EOPBAL_CAP,
--        CASE strleft(cod_gl,5) = '13801' and MONTH(CAST(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') AS TIMESTAMP)) 
--             WHEN 1 THEN CAST(B.GMCH01 AS DECIMAL(30,10))
--             WHEN 2 THEN CAST(B.GMCH02 AS DECIMAL(30,10))
--             WHEN 3 THEN CAST(B.GMCH03 AS DECIMAL(30,10))
--             WHEN 4 THEN CAST(B.GMCH04 AS DECIMAL(30,10))
--             WHEN 5 THEN CAST(B.GMCH05 AS DECIMAL(30,10))
--             WHEN 6 THEN CAST(B.GMCH06 AS DECIMAL(30,10))
--             WHEN 7 THEN CAST(B.GMCH07 AS DECIMAL(30,10))
--             WHEN 8 THEN CAST(B.GMCH08 AS DECIMAL(30,10))
--             WHEN 9 THEN CAST(B.GMCH09 AS DECIMAL(30,10))
--             WHEN 10 THEN CAST(B.GMCH10 AS DECIMAL(30,10))
--             WHEN 11 THEN CAST(B.GMCH11 AS DECIMAL(30,10))
--             WHEN 12 THEN CAST(B.GMCH12 AS DECIMAL(30,10)) END
        0 AS EOPBAL_INT,
--        CAST(A.gmmtda/(day(to_timestamp('${var:periodo}','yyyyMMdd'))-1) AS DECIMAL(30,10)) AS AVGBAL_CAP,
        0 AS AVGBAL_CAP,
--        CASE MONTH(CAST(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') AS TIMESTAMP)) 
--             WHEN 1 THEN CAST(B.GMCA01 AS DECIMAL(30,10))
--             WHEN 2 THEN CAST(B.GMCA02 AS DECIMAL(30,10))
--             WHEN 3 THEN CAST(B.GMCA03 AS DECIMAL(30,10))
--             WHEN 4 THEN CAST(B.GMCA04 AS DECIMAL(30,10))
--             WHEN 5 THEN CAST(B.GMCA05 AS DECIMAL(30,10))
--             WHEN 6 THEN CAST(B.GMCA06 AS DECIMAL(30,10))
--             WHEN 7 THEN CAST(B.GMCA07 AS DECIMAL(30,10))
--             WHEN 8 THEN CAST(B.GMCA08 AS DECIMAL(30,10))
--             WHEN 9 THEN CAST(B.GMCA09 AS DECIMAL(30,10))
--             WHEN 10 THEN CAST(B.GMCA10 AS DECIMAL(30,10))
--             WHEN 11 THEN CAST(B.GMCA11 AS DECIMAL(30,10))
--             WHEN 12 THEN CAST(B.GMCA12 AS DECIMAL(30,10)) END
        0 AS AVGBAL_INT,
        CASE WHEN strleft(A.GMACT,1) IN ('5','6') THEN CASE MONTH(CAST(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd') AS TIMESTAMP)) 
             WHEN 1 THEN CAST(B.GMCH01 AS DECIMAL(30,10))
             WHEN 2 THEN CAST(B.GMCH02 AS DECIMAL(30,10))
             WHEN 3 THEN CAST(B.GMCH03 AS DECIMAL(30,10))
             WHEN 4 THEN CAST(B.GMCH04 AS DECIMAL(30,10))
             WHEN 5 THEN CAST(B.GMCH05 AS DECIMAL(30,10))
             WHEN 6 THEN CAST(B.GMCH06 AS DECIMAL(30,10))
             WHEN 7 THEN CAST(B.GMCH07 AS DECIMAL(30,10))
             WHEN 8 THEN CAST(B.GMCH08 AS DECIMAL(30,10))
             WHEN 9 THEN CAST(B.GMCH09 AS DECIMAL(30,10))
             WHEN 10 THEN CAST(B.GMCH10 AS DECIMAL(30,10))
             WHEN 11 THEN CAST(B.GMCH11 AS DECIMAL(30,10))
             WHEN 12 THEN CAST(B.GMCH12 AS DECIMAL(30,10))
        ELSE 0
        END ELSE 0 END AS PL,
        'GLP00301' AS COD_INFO_SOURCE
FROM MIS_LOAD_GLP00301_MP AS A 
LEFT JOIN MIS_LOAD_GLP01301_MP AS B 
ON A.GMBK = B.GMBK AND A.GMACT = B.GMACT AND CAST(A.GMCURC AS INT) = CAST(B.GMCURC AS INT) AND A.GMCNTR = B.GMCNTR
;

--TRUNCATE TABLE IF EXISTS mis_load_gl_code_combinations;
--LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/GL_CODE_COMBINATIONS.csv' INTO TABLE mis_load_gl_code_combinations;

--TRUNCATE TABLE IF EXISTS mis_load_gl_je_headers;
--LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/GL_JE_HEADERS.csv' INTO TABLE mis_load_gl_je_headers;

--TRUNCATE TABLE IF EXISTS mis_load_gl_je_lines;
--LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/GL_JE_LINES.csv' INTO TABLE mis_load_gl_je_lines;

TRUNCATE TABLE IF EXISTS MIS_LOAD_SEGUROS;
LOAD DATA INPATH '${var:ruta_fuentes_contabilidad}/FUENTE_SEGUROS.csv' INTO TABLE MIS_LOAD_SEGUROS;

INSERT INTO MIS_APR_ACCOUNTING
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'SEG' AS COD_CONT, RPAD(TRIM(a.cod_gl),16,'0') AS COD_GL, a.des_gl AS DES_GL, NULL AS COD_ACCO_CENT,
NULL AS COD_OFFI, NULL AS COD_NAR, 'VIG' AS COD_BLCE_STATUS, case when a.cod_currency = '001' then 'HNL' 
when a.cod_currency = '002' then 'USD' when a.cod_currency = '003' then 'EUR' else 'HNL' end AS COD_CURRENCY, '2' AS COD_ENTITY, 
CAST(case when strleft(a.cod_gl,1) in ('1','2','3','6','7') then isnull(a.amount,0) else 0 end AS DECIMAL(30,10)) AS EOPBAL_CAP, 
0 AS EOPBAL_INT, CAST(CASE WHEN strleft(a.cod_gl,1) in ('1','2','3','6','7') then isnull(a.amount,0) else 0 end/(day(to_timestamp('${var:periodo}','yyyyMMdd'))) AS DECIMAL(30,10)) AS AVGBAL_CAP,
0 AS AVGBAL_INT, CAST(CASE WHEN strleft(a.cod_gl,1) in ('4','5') then isnull(a.amount,0) else 0 end AS DECIMAL(30,10)) AS PL, 'FUENTE_SEGUROS' AS COD_INFO_SOURCE
FROM MIS_LOAD_SEGUROS a
;

/*DROP TABLE IF EXISTS mis_tmp_gl_je_lines;
CREATE TABLE mis_tmp_gl_je_lines AS
SELECT sum(isnull(accounted_dr,0)) as accounted_dr, sum(isnull(accounted_cr,0))  as accounted_cr, segment3 as cod_gl, '${var:periodo}' as data_date
FROM mis_load_gl_je_lines a
left join mis_load_gl_code_combinations b
on cast(a.code_combination_id as string) = cast(b.code_combination_id as string)
WHERE status='P'
and TO_TIMESTAMP(effective_date, 'yyyy/MM/dd HH:mm:ss') between TRUNC(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),'MM') and seconds_sub(days_add(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'), 1), 1)
group by segment3
;*/

/*INSERT INTO MIS_APR_ACCOUNTING
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'SEG' AS COD_CONT, RPAD(TRIM(isnull(a.cod_gl,c.cod_gl_group)),16,'0') AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT,
NULL AS COD_OFFI, NULL AS COD_NAR, 'VIG' AS COD_BLCE_STATUS, 'HNL' AS COD_CURRENCY,
'2' AS COD_ENTITY, CAST(case when strleft(isnull(a.cod_gl,c.cod_gl_group),1) in ('1','2','3','6','7') then isnull(accounted_dr,0) - isnull(accounted_cr,0) + case when a.data_date = '20220630' OR (a.data_date is null and c.data_date = '20220531') then 0 when c.cod_gl_group = '3051030000000000' then 0 else isnull(c.eopbal_cap,0) end else 0 end AS DECIMAL(30,10)) AS EOPBAL_CAP, 0 AS EOPBAL_INT,
cast(CASE WHEN strleft(isnull(a.cod_gl,c.cod_gl_group),1) in ('1','2','3','6','7') then (isnull(accounted_dr,0) - isnull(accounted_cr,0) + case when a.data_date = '20220630' OR (a.data_date is null and c.data_date = '20220531') then 0 else isnull(c.eopbal_cap,0) end) else 0 end/(day(to_timestamp('${var:periodo}','yyyyMMdd'))) AS DECIMAL(30,10)) AS AVGBAL_CAP,
0 AS AVGBAL_INT, CAST(CASE WHEN strleft(a.cod_gl,1) in ('4','5') then isnull(accounted_dr,0) - isnull(accounted_cr,0) else 0 end AS DECIMAL(30,10)) AS PL, 'LINES' AS COD_INFO_SOURCE
FROM mis_tmp_gl_je_lines a
FULL JOIN (select data_date, cod_gl_group, sum(eopbal_cap) as eopbal_cap from mis_dm_balance_result_m where cod_entity = '2' and DATE_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),'MM'),1) = TO_TIMESTAMP(data_date,'yyyyMMdd')
GROUP BY data_date, cod_gl_group) c
ON a.cod_gl = c.cod_gl_group
--FULL JOIN mis_cierre_seguros_tmp c
--ON a.cod_gl = c.cuenta
;*/