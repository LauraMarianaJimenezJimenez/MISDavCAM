--------------------------------------------------------- MIS_APR_OFF_BALANCE --------------------------------------------------------
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

----Carga de tablas load

TRUNCATE TABLE IF EXISTS mis_load_gl_code_combinations;
LOAD DATA INPATH '${var:ruta_fuentes_seguros}/GL_CODE_COMBINATIONS.csv' INTO TABLE mis_load_gl_code_combinations;

TRUNCATE TABLE IF EXISTS mis_load_gl_je_headers;
LOAD DATA INPATH '${var:ruta_fuentes_seguros}/GL_JE_HEADERS.csv' INTO TABLE mis_load_gl_je_headers;

TRUNCATE TABLE IF EXISTS mis_load_gl_je_lines;
LOAD DATA INPATH '${var:ruta_fuentes_seguros}/GL_JE_LINES.csv' INTO TABLE mis_load_gl_je_lines;

DROP TABLE IF EXISTS mis_tmp_gl_je_lines;
CREATE TABLE mis_tmp_gl_je_lines AS
select sum(isnull(accounted_cr,0)) as accounted_cr, sum(isnull(accounted_dr,0)) as accounted_dr, a.context3 as cod_gl, '${var:periodo}' as data_date
from mis_load_gl_je_lines a
WHERE status='P' and TO_TIMESTAMP(effective_date, 'yyyy/MM/dd HH:mm:ss') between TRUNC(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),'MM') and TO_TIMESTAMP('${var:periodo}','yyyyMMdd')
GROUP BY a.code_combination_id
;

ALTER TABLE mis_apr_insurances
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

INSERT INTO mis_apr_insurances
PARTITION (DATA_DATE = '${var:periodo}')
SELECT 'SEG' AS COD_CONT, RPAD(TRIM(a.cod_gl),16,'0') AS COD_GL, NULL AS DES_GL, NULL AS COD_ACCO_CENT,
NULL AS COD_OFFI, NULL AS COD_NAR, 'VIG' AS COD_BLCE_STATUS, 'HNL' AS COD_CURRENCY,
'02' AS COD_ENTITY, CAST(CASE WHEN strleft(a.cod_gl,1) in ('1','2','3','4','7') then accounted_cr - accounted_dr + isnull(c.eopbal_cap,0) else 0 end AS DECIMAL(30,10)) AS EOPBAL_CAP, 0 AS EOPBAL_INT,
cast(CASE WHEN strleft(a.cod_gl,1) in ('1','2','3','4','7') then (accounted_cr - accounted_dr + isnull(c.eopbal_cap,0)) else 0 end/(day(to_timestamp('${var:periodo}','yyyyMMdd'))) AS DECIMAL(30,10)) AS AVGBAL_CAP,
0 AS AVGBAL_INT, CAST(CASE WHEN strleft(a.cod_gl,1) in ('5','6') then accounted_cr - accounted_dr + isnull(c.pl,0) else 0 end AS DECIMAL(30,10)) AS PL, 'LINES' AS COD_INFO_SOURCE
FROM mis_tmp_gl_je_lines a
LEFT JOIN (select * from mis_apr_insurances where DAYS_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),'MM'),1) = TO_TIMESTAMP(data_date,'yyyyMMdd')) c
ON a.cod_gl = c.cod_gl
WHERE a.DATA_DATE = '${var:periodo}'
;