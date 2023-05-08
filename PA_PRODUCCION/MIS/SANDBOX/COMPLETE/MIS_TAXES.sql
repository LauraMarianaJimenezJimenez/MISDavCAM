----------------------------------------------------------------------------- MOTOR DE IMPUESTOS -----------------------------------------------------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Eliminación de registros presentes en la parametría ---

DROP TABLE IF EXISTS MIS_DM_BALANCE_TMP_DELETE;
CREATE TABLE MIS_DM_BALANCE_TMP_DELETE AS
SELECT *
FROM MIS_DM_BALANCE_RESULT A
WHERE DATA_DATE = '{var:periodo}' AND COD_INFO_SOURCE <> 'RCCLIM'
;

ALTER TABLE MIS_DM_BALANCE_RESULT
DROP IF EXISTS PARTITION (DATA_DATE = '{var:periodo}');

INSERT INTO MIS_DM_BALANCE_RESULT PARTITION(DATA_DATE)
SELECT *
FROM MIS_DM_BALANCE_TMP_DELETE A;

--TRUNCATE TABLE IF EXISTS MIS_DM_BALANCE_TMP_DELETE;
--DROP TABLE IF EXISTS MIS_DM_BALANCE_TMP_DELETE;

----------- Adicion de parametros para el motor de impuestos ------------

DROP TABLE IF EXISTS MIS_TMP_TAXES_PAR;
CREATE TABLE MIS_TMP_TAXES_PAR AS 
	SELECT a.COD_ENTITY, a.COD_GL_GROUP_DESTINY AS COD_GL_GROUP, a.COD_ACCO_CENT_DESTINY AS COD_ACCO_CENT, a.TAX_PERC, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, b.COD_BLCE_STATUS, d.COD_PL_ACC, e.DES_PL_ACC, 
	f.COD_BLCE_PROD, g.DES_BLCE_PROD 
	FROM MIS_PAR_TAX_ENG a 
	LEFT JOIN MIS_PAR_CAT_CAF b 
	ON a.COD_GL_GROUP_DESTINY = b.COD_GL_GROUP
	LEFT JOIN MIS_PAR_REL_PL_ACC d 
	ON a.COD_ENTITY = d.COD_ENTITY AND a.COD_GL_GROUP_DESTINY = d.COD_GL_GROUP
	LEFT JOIN MIS_PAR_CAT_PL e
    ON d.COD_PL_ACC = e.COD_PL_ACC
	LEFT JOIN MIS_PAR_REL_BP_ACC f 
   	ON a.COD_ENTITY = f.COD_ENTITY AND a.COD_GL_GROUP_DESTINY = f.COD_GL_GROUP
	LEFT JOIN MIS_PAR_CAT_BP g
	ON f.COD_BLCE_PROD = g.COD_BLCE_PROD
   	;

------------ Union de contratos y movimientos por calcular impuesto ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_PL;
CREATE TABLE MIS_TMP_TAXES_PL AS
	SELECT DWH.DATA_DATE, DWH.COD_ENTITY, DWH.PL, DWH.COD_BUSINESS_LINE, ISNULL(DWH.DES_BUSINESS_LINE,'') DES_BUSINESS_LINE
	FROM 
		(SELECT DATA_DATE, COD_ENTITY, PL_TOT_RPT AS PL, COD_BUSINESS_LINE, NULL AS DES_BUSINESS_LINE, IDF_CTO, COD_PRODUCT, COD_SUBPRODUCT
		FROM MIS_DM_BALANCE_RESULT
		WHERE DATA_DATE='${var:periodo}' AND (PL_TOT_RPT<>0 OR FTP_RESULT_RPT<>0)
		) DWH
	LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') DT
	ON DWH.DATA_DATE=DT.DATA_DATE AND DWH.IDF_CTO=DT.IDF_CTO AND DWH.COD_ENTITY=DT.COD_ENTITY AND DWH.COD_PRODUCT=DT.COD_PRODUCT AND DWH.COD_SUBPRODUCT=DT.COD_SUBPRODUCT;
	


------------ Suma de valor de contratos y cálculo del impuesto ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_CONS;
CREATE TABLE MIS_TMP_TAXES_CONS AS
	SELECT a.*, b.COD_GL_GROUP, b.COD_ACCO_CENT, b.TAX_PERC, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, b.COD_BLCE_STATUS, b.COD_PL_ACC, b.DES_PL_ACC, b.COD_BLCE_PROD, b.DES_BLCE_PROD,
	CAST((-1 * IFNULL(a.PL, 0) * b.TAX_PERC/c.CONTEO) AS DECIMAL(30, 10)) AS IMPUESTO_PL
	FROM 
		(SELECT DATA_DATE, COD_ENTITY, CAST(SUM(PL) AS DECIMAL(30, 10)) AS PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE
		FROM MIS_TMP_TAXES_PL
		GROUP BY DATA_DATE, COD_ENTITY, COD_BUSINESS_LINE, DES_BUSINESS_LINE) a
	INNER JOIN 
		MIS_TMP_TAXES_PAR b 
		ON a.COD_ENTITY = b.COD_ENTITY
        LEFT JOIN 
	        (SELECT COD_ENTITY, COUNT(*) CONTEO FROM MIS_PAR_TAX_ENG GROUP BY COD_ENTITY) c
	        ON a.COD_ENTITY = c.COD_ENTITY
;

------------ Tabla Temporal con impuestos calculados para proceder a hacer conciliación ------------
DROP TABLE IF EXISTS MIS_DM_BALANCE_TMP_TAXES;
CREATE TABLE MIS_DM_BALANCE_TMP_TAXES AS
	SELECT a.COD_ACCO_CENT, a.COD_BLCE_STATUS, a.COD_ENTITY, 0 AS EOPBAL_CAP, 0 AS EOPBAL_INT, 0 AS AVGBAL_CAP, 0 AS AVGBAL_INT,
	CAST(SUM(a.IMPUESTO_PL) AS DECIMAL(30, 10)) AS PL,
	'RCCLIM' AS COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, CAST(NULL AS STRING) AS FTP,
	CAST(NULL AS STRING) AS FTP_RESULT, CAST(NULL AS STRING) AS IND_POOL, CAST(NULL AS STRING) AS COD_SEGMENT, CAST(NULL AS STRING) AS DES_SEGMENT, a.DATA_DATE AS DATA_DATE, 'RCIM' AS COD_CONT 
	FROM MIS_TMP_TAXES_CONS a
	WHERE a.COD_PL_ACC IS NOT NULL AND a.COD_BLCE_PROD IS NOT NULL
	GROUP BY a.DATA_DATE, a.COD_ACCO_CENT, a.COD_BLCE_STATUS, a.COD_ENTITY, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, 
	a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE;

--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_PAR;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_PAR;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_PL;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_PL;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONS;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONS;

------------------------------------------------------- CONCILIACION --------------------------------------------------------------------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC;
CREATE TABLE MIS_TMP_TAXES_CONC AS
	SELECT 
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.DATA_DATE
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.DATA_DATE 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.DATA_DATE 
		END AS DATA_DATE,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN 'RCCL'
        WHEN CONT.DATA_DATE IS NOT NULL THEN IF(COD_OPER.COD_GL_GROUP IS NOT NULL, 'RCTB', 'CTBP')
        WHEN OPER.DATA_DATE IS NOT NULL THEN 'RCCL' 
        END AS COD_CONT,
        CASE 
        WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ACCO_CENT
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ACCO_CENT 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_ACCO_CENT 
		END AS COD_ACCO_CENT,
		CONT.COD_CURRENCY AS COD_CURRENCY,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ENTITY
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_ENTITY 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_ENTITY 
		END AS COD_ENTITY,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_GL_GROUP
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_GL_GROUP 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_GL_GROUP 
		END AS COD_GL_GROUP,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.DES_GL_GROUP
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.DES_GL_GROUP 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.DES_GL_GROUP 
		END AS DES_GL_GROUP,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_CAP - OPER.TOT_EOPBAL_CAP) AS DECIMAL(30, 10)) 
		WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_CAP) AS DECIMAL(30, 10))
		WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_EOPBAL_CAP) AS DECIMAL(30, 10)) 
		END AS EOPBAL_CAP,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_INT - OPER.TOT_EOPBAL_INT) AS DECIMAL(30, 10)) 
		WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_EOPBAL_INT) AS DECIMAL(30, 10))
		WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_EOPBAL_INT) AS DECIMAL(30, 10)) 
		END AS EOPBAL_INT,
		0 AS AVGBAL_CAP,
		0 AS AVGBAL_INT,
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_PL - OPER.TOT_PL) AS DECIMAL(30, 10)) 
		WHEN CONT.DATA_DATE IS NOT NULL THEN CAST((CONT.TOT_PL) AS DECIMAL(30, 10))
		WHEN OPER.DATA_DATE IS NOT NULL THEN CAST((-1 * OPER.TOT_PL) AS DECIMAL(30, 10)) 
		END AS PL 
	FROM 
		(SELECT DATA_DATE, COD_ACCO_CENT, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP, SUM(EOPBAL_CAP) AS TOT_EOPBAL_CAP, SUM(EOPBAL_INT) AS TOT_EOPBAL_INT, SUM(PL) AS TOT_PL 
		FROM MIS_DM_BALANCE_TMP_TAXES
		WHERE DATA_DATE='${var:periodo}' AND COD_CONT ='RCIM'
		GROUP BY DATA_DATE, COD_ACCO_CENT, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP) OPER 
	FULL JOIN 
		(SELECT DATA_DATE, NULL AS COD_ACCO_CENT, COD_CURRENCY, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP, SUM(EOPBAL_CAP) AS TOT_EOPBAL_CAP, SUM(EOPBAL_INT) AS TOT_EOPBAL_INT, SUM(PL) AS TOT_PL
		FROM MIS_DWH_RECONCILIATION 
		WHERE DATA_DATE='${var:periodo}' AND COD_CONT IN('RCTB','CTBP')
		AND COD_GL_GROUP IN (SELECT DISTINCT COD_GL_GROUP_DESTINY FROM MIS_PAR_TAX_ENG) 
		AND COD_ENTITY IN (SELECT DISTINCT COD_ENTITY FROM MIS_PAR_TAX_ENG) 
		GROUP BY DATA_DATE, COD_CURRENCY, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP) CONT 
		ON OPER.DATA_DATE = CONT.DATA_DATE AND OPER.COD_ENTITY = CONT.COD_ENTITY AND OPER.COD_GL_GROUP = CONT.COD_GL_GROUP
   	LEFT JOIN 
   		(SELECT DISTINCT COD_GL_GROUP FROM MIS_PAR_REL_CAF_OPER) COD_OPER
		ON CONT.COD_GL_GROUP = COD_OPER.COD_GL_GROUP;  

------------ Adicion de parámetros  ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC_AUX;
CREATE TABLE MIS_TMP_TAXES_CONC_AUX AS
SELECT a.DATA_DATE, a.COD_CONT, a.COD_ACCO_CENT, '0' AS COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.EOPBAL_CAP, a.EOPBAL_INT, 0 AS AVGBAL_CAP, 0 AS AVGBAL_INT, a.PL, 'RCCLIM' AS COD_INFO_SOURCE,
a.COD_GL_GROUP, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, c.COD_PL_ACC, d.DES_PL_ACC, e.COD_BLCE_PROD, f.DES_BLCE_PROD, g.COD_BUSINESS_LINE, h.DES_BUSINESS_LINE, CAST(NULL AS STRING) AS FTP, CAST(NULL AS STRING) AS FTP_RESULT, CAST(NULL AS STRING) AS IND_POOL, CAST(NULL AS STRING) AS COD_SEGMENT, CAST(NULL AS STRING) AS DES_SEGMENT 
FROM 
(SELECT * FROM MIS_TMP_TAXES_CONC 
WHERE EOPBAL_CAP <> 0 OR EOPBAL_INT <> 0 OR PL <> 0) a 
LEFT JOIN 
(SELECT DISTINCT COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT FROM MIS_PAR_CAT_CAF) b
ON a.COD_GL_GROUP = b.COD_GL_GROUP 
LEFT JOIN 
MIS_PAR_REL_PL_ACC c 
ON a.COD_ENTITY = c.COD_ENTITY AND a.COD_GL_GROUP = c.COD_GL_GROUP
LEFT JOIN MIS_PAR_CAT_PL d
ON c.COD_PL_ACC = d.COD_PL_ACC
LEFT JOIN MIS_PAR_REL_BP_ACC  e 
ON a.COD_ENTITY = e.COD_ENTITY AND a.COD_GL_GROUP = e.COD_GL_GROUP
LEFT JOIN MIS_PAR_CAT_BP f
ON e.COD_BLCE_PROD = f.COD_BLCE_PROD
LEFT JOIN MIS_PAR_REL_BL_ACC g
ON a.COD_ENTITY = g.COD_ENTITY AND a.COD_GL_GROUP = g.COD_GL_GROUP	
LEFT JOIN MIS_PAR_CAT_BL h
ON g.COD_BUSINESS_LINE = h.COD_BUSINESS_LINE
WHERE a.DATA_DATE='${var:periodo}';

------------ Inserción de registros de impuestos -----------
INSERT INTO MIS_DM_BALANCE_RESULT 
(COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, PL_TOT, PL_TOT_RPT, PL_RPT, COD_INFO_SOURCE, COD_GL_GROUP, 
DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, IND_POOL, COD_SEGMENT, DES_SEGMENT, EXP_TYPE, EXP_FAMILY, COD_CONT) 
PARTITION(DATA_DATE)
SELECT A.COD_ACCO_CENT, A.COD_BLCE_STATUS, 'HNL' AS COD_CURRENCY, A.COD_ENTITY, A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, CAST(A.PL AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), 
CAST(A.PL AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), A.COD_INFO_SOURCE, A.COD_GL_GROUP, 
A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, 
NULL AS EXP_TYPE, NULL AS EXP_FAMILY, A.COD_CONT, A.DATA_DATE
FROM MIS_DM_BALANCE_TMP_TAXES A;

------------ Inserción de registros conciliados -----------
INSERT INTO MIS_DM_BALANCE_RESULT 
(COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, PL_TOT, PL_TOT_RPT, PL_RPT, COD_INFO_SOURCE, COD_GL_GROUP, 
DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, IND_POOL, COD_SEGMENT, DES_SEGMENT, EXP_TYPE, EXP_FAMILY, COD_CONT) 
PARTITION(DATA_DATE)
SELECT A.COD_ACCO_CENT, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, CAST(A.EOPBAL_CAP AS DECIMAL(30,2)), CAST(A.EOPBAL_INT AS DECIMAL(30,2)), CAST(A.AVGBAL_CAP AS DECIMAL(30,2)), 
CAST(A.AVGBAL_INT AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), CAST(A.PL AS DECIMAL(30,2)), A.COD_INFO_SOURCE, A.COD_GL_GROUP, 
A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, 
NULL AS EXP_TYPE, NULL AS EXP_FAMILY, A.COD_CONT, A.DATA_DATE
FROM MIS_TMP_TAXES_CONC_AUX A
WHERE a.COD_PL_ACC IS NOT NULL AND a.COD_BLCE_PROD IS NOT NULL;

------------ Inserción de contrapartida de la contabilidad de impuestos -----------
INSERT INTO MIS_DM_BALANCE_RESULT 
(COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, PL_TOT, PL_TOT_RPT, PL_RPT, COD_INFO_SOURCE, COD_GL_GROUP, 
DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, IND_POOL, COD_SEGMENT, DES_SEGMENT, EXP_TYPE, EXP_FAMILY, COD_CONT) 
PARTITION(DATA_DATE)
SELECT COD_ACCO_CENT, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(-1*PL AS DECIMAL(30,2)) AS PL, CAST(-1*PL_TOT AS DECIMAL(30,2)) AS PL_TOT, 
CAST(-1*PL_TOT_RPT AS DECIMAL(30,2)) AS PL_TOT_RPT, CAST(-1*PL_RPT AS DECIMAL(30,2)) AS PL_RPT, COD_INFO_SOURCE, COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, 
DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, IND_POOL, COD_SEGMENT, DES_SEGMENT, NULL AS EXP_TYPE, NULL AS EXP_FAMILY, 'RCCL' AS COD_CONT, DATA_DATE
FROM MIS_DM_BALANCE_RESULT
WHERE DATA_DATE='${var:periodo}' AND COD_CONT IN('RCTB','CTBP')
AND COD_GL_GROUP IN (SELECT DISTINCT COD_GL_GROUP_DESTINY FROM MIS_PAR_TAX_ENG) 
AND COD_ENTITY IN (SELECT DISTINCT COD_ENTITY FROM MIS_PAR_TAX_ENG);



--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONC;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONC_AUX;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC_AUX;
--TRUNCATE TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES;
--DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES;