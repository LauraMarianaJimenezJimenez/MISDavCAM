---------- MOTOR DE IMPUESTOS ----------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Eliminación de registros previos de Impuestos ---
ALTER TABLE MIS_DWH_RECONCILIATION
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}', COD_CONT='RCIM');

------------ Creación de Tabla Destino de impuestos -----------
DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_DELETE;
CREATE TABLE IF NOT EXISTS MIS_DWH_RECONCILIATION_TMP_DELETE (
 COD_ACCO_CENT      STRING
,COD_OFFI           STRING
,COD_NAR            STRING
,COD_BLCE_STATUS    STRING
,COD_CURRENCY       STRING
,COD_ENTITY         STRING
,EOPBAL_CAP         DECIMAL(30,10)
,EOPBAL_INT         DECIMAL(30,10)
,AVGBAL_CAP         DECIMAL(30,10)
,AVGBAL_INT         DECIMAL(30,10)
,PL                 DECIMAL(30,10)
,COD_INFO_SOURCE    STRING
,COD_GL_GROUP       STRING
,DES_GL_GROUP       STRING
,ACCOUNT_CONCEPT    STRING
,COD_PL_ACC         STRING
,DES_PL_ACC         STRING
,COD_BLCE_PROD      STRING
,DES_BLCE_PROD      STRING
,COD_BUSINESS_LINE  STRING
,DES_BUSINESS_LINE  STRING
,FTP                DECIMAL(30,10)
,FTP_RESULT         DECIMAL(30,10) 
,IND_POOL          STRING
,COD_SEGMENT       STRING
,DES_SEGMENT       STRING
,EXP_TYPE          STRING
,EXP_FAMILY        STRING
,DATA_DATE         STRING
,COD_CONT          STRING);

--- Eliminación de registros presentes en la parametría ---

INSERT INTO MIS_DWH_RECONCILIATION_TMP_DELETE
SELECT A.COD_ACCO_CENT, A.COD_OFFI, A.COD_NAR, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, A.COD_INFO_SOURCE, A.COD_GL_GROUP, A.DES_GL_GROUP, A.ACCOUNT_CONCEPT,
A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.FTP, A.FTP_RESULT, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, A.EXP_TYPE, A.EXP_FAMILY, A.DATA_DATE, A.COD_CONT
FROM MIS_DWH_RECONCILIATION A
WHERE DATA_DATE = '{var:periodo}' AND COD_INFO_SOURCE <> 'RCCLIM'
;

ALTER TABLE MIS_DWH_RECONCILIATION
DROP IF EXISTS PARTITION (DATA_DATE = '{var:periodo}');

INSERT INTO MIS_DWH_RECONCILIATION PARTITION(DATA_DATE, COD_CONT)
SELECT A.COD_ACCO_CENT, A.COD_OFFI, A.COD_NAR, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, A.COD_INFO_SOURCE, A.COD_GL_GROUP, A.DES_GL_GROUP, A.ACCOUNT_CONCEPT,
A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.FTP, A.FTP_RESULT, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, A.EXP_TYPE, A.EXP_FAMILY, A.DATA_DATE, A.COD_CONT
FROM MIS_DWH_RECONCILIATION_TMP_DELETE A;

--TRUNCATE TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_DELETE;
--DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_DELETE;

----------- Adicion de parametros para el motor de impuestos ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_PAR;
CREATE TABLE IF NOT EXISTS MIS_TMP_TAXES_PAR(
 COD_ENTITY                STRING
,COD_GL_GROUP              STRING
,COD_ACCO_CENT             STRING
,TAX_PERC                  DECIMAL(30, 10)
,DES_GL_GROUP              STRING
,ACCOUNT_CONCEPT           STRING
,COD_BLCE_STATUS           STRING
,COD_PL_ACC                STRING
,DES_PL_ACC                STRING
,COD_BLCE_PROD             STRING
,DES_BLCE_PROD             STRING);

INSERT INTO MIS_TMP_TAXES_PAR 
	SELECT a.COD_ENTITY, a.COD_GL_GROUP_DESTINY AS COD_GL_GROUP, a.COD_ACCO_CENT_DESTINY AS COD_ACCO_CENT, a.TAX_PERC, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, b.COD_BLCE_STATUS, d.COD_PL_ACC, e.DES_PL_ACC, 
	f.COD_BLCE_PROD, g.DES_BLCE_PROD 
	FROM MIS_PAR_TAX_ENG a 
	LEFT JOIN MIS_PAR_CAT_CAF b 
	ON a.COD_GL_GROUP_DESTINY = b.COD_GL_GROUP
	LEFT JOIN MIS_PAR_REL_PL_ACC d 
	ON a.COD_ENTITY = d.COD_ENTITY AND a.COD_GL_GROUP_DESTINY = d.COD_GL_GROUP AND d.COD_CURRENCY = 'HNL'
	LEFT JOIN MIS_PAR_CAT_PL e
    ON d.COD_PL_ACC = e.COD_PL_ACC
	LEFT JOIN MIS_PAR_REL_BP_ACC f 
   	ON a.COD_ENTITY = f.COD_ENTITY AND a.COD_GL_GROUP_DESTINY = f.COD_GL_GROUP AND f.COD_CURRENCY = 'HNL'
	LEFT JOIN MIS_PAR_CAT_BP g
	ON f.COD_BLCE_PROD = g.COD_BLCE_PROD
   	;

------------ Union de contratos y movimientos por calcular impuesto ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_PL;
CREATE TABLE IF NOT EXISTS MIS_TMP_TAXES_PL (
 DATA_DATE	        STRING
,COD_CURRENCY	    STRING
,COD_ENTITY	        STRING
,PL	                DECIMAL(30, 10)
,COD_BUSINESS_LINE	STRING
,DES_BUSINESS_LINE	STRING
,FTP_RESULT	        DECIMAL(30, 10));

INSERT INTO MIS_TMP_TAXES_PL
	SELECT DWH.DATA_DATE, DWH.COD_CURRENCY, DWH.COD_ENTITY, DWH.PL, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, DWH.FTP_RESULT 
	FROM 
		(SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, IDF_CTO, COD_PRODUCT, COD_SUBPRODUCT
		FROM MIS_DWH_ASSETS
		WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
        UNION ALL
        SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, IDF_CTO, COD_PRODUCT, COD_SUBPRODUCT
		FROM MIS_DWH_LIABILITIES
		WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
		UNION ALL
        SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, IDF_CTO, COD_PRODUCT, COD_SUBPRODUCT
		FROM MIS_DWH_FEES
		WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
		UNION ALL
        SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, IDF_CTO, COD_PRODUCT, COD_SUBPRODUCT
		FROM MIS_DWH_PROVISIONS
		WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
		UNION ALL
        SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, NULL AS IDF_CTO, NULL AS COD_PRODUCT, NULL AS COD_SUBPRODUCT
		FROM MIS_DWH_RECONCILIATION
		WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
                UNION ALL
        SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP_RESULT, NULL AS IDF_CTO, NULL AS COD_PRODUCT, NULL AS COD_SUBPRODUCT
                FROM MIS_DWH_EXPENSES
                WHERE DATA_DATE='${var:periodo}' AND (PL<>0 OR FTP_RESULT<>0)
		) DWH
	LEFT JOIN MIS_APR_CONTRACT_DT DT
	ON DWH.DATA_DATE=DT.DATA_DATE AND DWH.IDF_CTO=DT.IDF_CTO AND DWH.COD_ENTITY=DT.COD_ENTITY AND DWH.COD_PRODUCT=DT.COD_PRODUCT AND DWH.COD_SUBPRODUCT=DT.COD_SUBPRODUCT 
	AND DWH.COD_CURRENCY=DT.COD_CURRENCY;

------------ Suma de valor de contratos y cálculo del impuesto ------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_CONS;
CREATE TABLE IF NOT EXISTS MIS_TMP_TAXES_CONS(
 DATA_DATE	         STRING
,COD_CURRENCY	     STRING
,COD_ENTITY	         STRING
,PL	                 DECIMAL(30, 10)
,COD_BUSINESS_LINE   STRING
,DES_BUSINESS_LINE   STRING
,FTP_RESULT	         DECIMAL(30, 10)
,COD_GL_GROUP        STRING
,COD_ACCO_CENT       STRING
,TAX_PERC            DECIMAL(30, 10)
,DES_GL_GROUP        STRING
,ACCOUNT_CONCEPT     STRING
,COD_BLCE_STATUS     STRING
,COD_PL_ACC          STRING
,DES_PL_ACC          STRING
,COD_BLCE_PROD       STRING
,DES_BLCE_PROD       STRING
,IMPUESTO_PL         DECIMAL(30, 10)
,IMPUESTO_FTP_RESULT DECIMAL(30, 10)); 


INSERT INTO MIS_TMP_TAXES_CONS
	SELECT a.*, b.COD_GL_GROUP, b.COD_ACCO_CENT, b.TAX_PERC, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, b.COD_BLCE_STATUS, b.COD_PL_ACC, b.DES_PL_ACC, b.COD_BLCE_PROD, b.DES_BLCE_PROD,
	CAST((-1 * IFNULL(a.PL, 0) * b.TAX_PERC) AS DECIMAL(30, 10)) AS IMPUESTO_PL, CAST((-1 * IFNULL(a.FTP_RESULT, 0) * b.TAX_PERC) AS DECIMAL(30, 10)) AS IMPUESTO_FTP_RESULT
	FROM 
		(SELECT DATA_DATE, COD_CURRENCY, COD_ENTITY, CAST(SUM(PL) AS DECIMAL(30, 10)) AS PL, COD_BUSINESS_LINE, DES_BUSINESS_LINE, CAST(SUM(FTP_RESULT) AS DECIMAL(30, 10)) AS FTP_RESULT
		FROM MIS_TMP_TAXES_PL
		GROUP BY DATA_DATE, COD_CURRENCY, COD_ENTITY, COD_BUSINESS_LINE, DES_BUSINESS_LINE) a
	INNER JOIN 
		MIS_TMP_TAXES_PAR b 
		ON a.COD_ENTITY = b.COD_ENTITY;

------------ Tabla Temporal con impuestos calculados para proceder a hacer conciliación ------------
DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES;
CREATE TABLE IF NOT EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES (
 COD_ACCO_CENT      STRING
,COD_OFFI           STRING
,COD_NAR            STRING
,COD_BLCE_STATUS    STRING
,COD_CURRENCY       STRING
,COD_ENTITY         STRING
,EOPBAL_CAP         DECIMAL(30,10)
,EOPBAL_INT         DECIMAL(30,10)
,AVGBAL_CAP         DECIMAL(30,10)
,AVGBAL_INT         DECIMAL(30,10)
,PL                 DECIMAL(30,10)
,COD_INFO_SOURCE    STRING
,COD_GL_GROUP       STRING
,DES_GL_GROUP       STRING
,ACCOUNT_CONCEPT    STRING
,COD_PL_ACC         STRING
,DES_PL_ACC         STRING
,COD_BLCE_PROD      STRING
,DES_BLCE_PROD      STRING
,COD_BUSINESS_LINE  STRING
,DES_BUSINESS_LINE  STRING
,FTP                DECIMAL(30,10)
,FTP_RESULT         DECIMAL(30,10) 
,IND_POOL           STRING
,COD_SEGMENT        STRING
,DES_SEGMENT        STRING
,DATA_DATE          STRING
,COD_CONT           STRING);


INSERT INTO MIS_DWH_RECONCILIATION_TMP_TAXES
	SELECT a.COD_ACCO_CENT, NULL AS COD_OFFI, NULL AS COD_NAR, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, 0 AS EOPBAL_CAP, 0 AS EOPBAL_INT, 0 AS AVGBAL_CAP, 0 AS AVGBAL_INT,
	CAST(SUM(a.IMPUESTO_PL+a.IMPUESTO_FTP_RESULT) AS DECIMAL(30, 10)) AS PL,
	'RCCLIM' AS COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, NULL AS FTP,
	NULL AS FTP_RESULT, NULL AS IND_POOL, NULL AS COD_SEGMENT, NULL AS DES_SEGMENT, a.DATA_DATE AS DATA_DATE, 'RCIM' AS COD_CONT 
	FROM MIS_TMP_TAXES_CONS a
	WHERE a.COD_PL_ACC IS NOT NULL AND a.COD_BLCE_PROD IS NOT NULL
	GROUP BY a.DATA_DATE, a.COD_ACCO_CENT, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, 
	a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE;

------------ Inserción de registros rechazados -----------
/*INSERT INTO MIS_REJECTIONS_ALLOCATIONS PARTITION (COD_ENG)
	SELECT a.DATA_DATE, 'IMPUESTOS' AS DES_REJ, 'N/A' AS COD_BLCE_STATUS,  a.COD_PL_ACC, a.COD_BLCE_PROD, SUM(a.PL) AS PL, SUM(a.FTP_RESULT) AS FTP_RESULT, 'TAX' AS COD_ENG
	FROM MIS_TMP_TAXES_CONS a
	WHERE a.COD_PL_ACC IS NULL OR a.COD_BLCE_PROD IS NULL
	GROUP BY a.DATA_DATE, a.COD_PL_ACC, a.COD_BLCE_PROD;
*/
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_PAR;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_PAR;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_PL;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_PL;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONS;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONS;

--- Tabla con valores contravalorados ---

DROP TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL_RPT_TAXES;
CREATE TABLE IF NOT EXISTS MIS_TMP_DWH_OPERACIONAL_RPT_TAXES(
 COD_ACCO_CENT      STRING
,COD_OFFI           STRING
,COD_NAR            STRING
,COD_BLCE_STATUS    STRING
,COD_CURRENCY       STRING
,COD_ENTITY         STRING
,EOPBAL_CAP         DECIMAL(30,10)
,EOPBAL_INT         DECIMAL(30,10)
,AVGBAL_CAP         DECIMAL(30,10)
,AVGBAL_INT         DECIMAL(30,10)
,PL                 DECIMAL(30,10)
,COD_INFO_SOURCE    STRING
,COD_GL_GROUP       STRING
,DES_GL_GROUP       STRING
,ACCOUNT_CONCEPT    STRING
,COD_PL_ACC         STRING
,DES_PL_ACC         STRING
,COD_BLCE_PROD      STRING
,DES_BLCE_PROD      STRING
,COD_BUSINESS_LINE  STRING
,DES_BUSINESS_LINE  STRING
,FTP                DECIMAL(30,10)
,FTP_RESULT         DECIMAL(30,10) 
,IND_POOL           STRING
,COD_SEGMENT        STRING
,DES_SEGMENT        STRING
,DATA_DATE          STRING
,COD_CONT           STRING);


INSERT INTO MIS_TMP_DWH_OPERACIONAL_RPT_TAXES
SELECT DWH.COD_ACCO_CENT, DWH.COD_OFFI, DWH.COD_NAR, DWH.COD_BLCE_STATUS,  'HNL' AS COD_CURRENCY, DWH.COD_ENTITY,
CAST(CASE WHEN DWH.COD_CURRENCY='HNL' THEN DWH.EOPBAL_CAP ELSE DWH.EOPBAL_CAP * RT.EXCH_RATE END AS DECIMAL(30,10)) AS EOPBAL_CAP,
CAST(CASE WHEN DWH.COD_CURRENCY='HNL' THEN DWH.EOPBAL_INT ELSE DWH.EOPBAL_INT * RT.EXCH_RATE END AS DECIMAL(30,10)) AS EOPBAL_INT,
DWH.AVGBAL_CAP, DWH.AVGBAL_INT, 
CAST(CASE WHEN DWH.COD_CURRENCY='HNL' THEN DWH.PL ELSE DWH.PL * RT.EXCH_RATE END AS DECIMAL(30,10)) AS PL, DWH.COD_INFO_SOURCE, DWH.COD_GL_GROUP, DWH.DES_GL_GROUP, DWH.ACCOUNT_CONCEPT, DWH.COD_PL_ACC, DWH.DES_PL_ACC, DWH.COD_BLCE_PROD, DWH.DES_BLCE_PROD, DWH.COD_BUSINESS_LINE, DWH.DES_BUSINESS_LINE, DWH.FTP,
	DWH.FTP_RESULT, DWH.IND_POOL, DWH.COD_SEGMENT, DWH.DES_SEGMENT, DWH.DATA_DATE, DWH.COD_CONT
FROM MIS_DWH_RECONCILIATION_TMP_TAXES DWH 
LEFT JOIN MIS_PAR_EXCH_RATE RT ON DWH.DATA_DATE = RT.DATA_DATE 
AND DWH.COD_CURRENCY = RT.COD_CURRENCY AND RT.COD_CONT = 'LOCAL'
;

------------------------------------------------------- CONCILIACION --------------------------------------------------------------------------
DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC;
CREATE TABLE IF NOT EXISTS MIS_TMP_TAXES_CONC(
 DATA_DATE       STRING
,COD_CONT	     STRING
,COD_ACCO_CENT   STRING
,COD_CURRENCY    STRING
,COD_ENTITY      STRING
,COD_GL_GROUP    STRING
,DES_GL_GROUP    STRING
,EOPBAL_CAP      DECIMAL(30, 10)
,EOPBAL_INT      DECIMAL(30, 10)
,AVGBAL_CAP      DECIMAL(30, 10)
,AVGBAL_INT      DECIMAL(30, 10)
,PL              DECIMAL(30, 10));

INSERT INTO MIS_TMP_TAXES_CONC
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
		CASE 
		WHEN OPER.DATA_DATE IS NOT NULL AND CONT.DATA_DATE IS NOT NULL THEN CONT.COD_CURRENCY
		WHEN CONT.DATA_DATE IS NOT NULL THEN CONT.COD_CURRENCY 
		WHEN OPER.DATA_DATE IS NOT NULL THEN OPER.COD_CURRENCY 
		END AS COD_CURRENCY,
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
		(SELECT DATA_DATE, COD_ACCO_CENT, COD_CURRENCY, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP, SUM(EOPBAL_CAP) AS TOT_EOPBAL_CAP, SUM(EOPBAL_INT) AS TOT_EOPBAL_INT, SUM(PL) AS TOT_PL 
		FROM MIS_TMP_DWH_OPERACIONAL_RPT_TAXES
		WHERE DATA_DATE='${var:periodo}' AND COD_CONT ='RCIM'
		GROUP BY DATA_DATE, COD_ACCO_CENT, COD_CURRENCY, COD_ENTITY, COD_GL_GROUP, DES_GL_GROUP) OPER 
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
CREATE TABLE IF NOT EXISTS MIS_TMP_TAXES_CONC_AUX(
 DATA_DATE           STRING
,COD_CONT            STRING
,COD_ACCO_CENT       STRING
,COD_BLCE_STATUS     STRING
,COD_CURRENCY        STRING
,COD_ENTITY          STRING
,EOPBAL_CAP          DECIMAL(30, 10)
,EOPBAL_INT          DECIMAL(30, 10)
,AVGBAL_CAP          DECIMAL(30, 10)
,AVGBAL_INT          DECIMAL(30, 10)
,PL                  DECIMAL(30, 10)
,COD_INFO_SOURCE     STRING
,COD_GL_GROUP        STRING
,DES_GL_GROUP        STRING
,ACCOUNT_CONCEPT     STRING
,COD_PL_ACC          STRING
,DES_PL_ACC          STRING
,COD_BLCE_PROD       STRING
,DES_BLCE_PROD       STRING
,COD_BUSINESS_LINE   STRING
,DES_BUSINESS_LINE   STRING
,FTP                 DECIMAL(30, 10)
,FTP_RESULT          DECIMAL(30, 10)
,IND_POOL            STRING
,COD_SEGMENT         STRING
,DES_SEGMENT         STRING
);

INSERT INTO MIS_TMP_TAXES_CONC_AUX 
	SELECT a.DATA_DATE, a.COD_CONT, a.COD_ACCO_CENT, '0' AS COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.EOPBAL_CAP, a.EOPBAL_INT, 0 AS AVGBAL_CAP, 0 AS AVGBAL_INT, a.PL, 'RCCLIM' AS COD_INFO_SOURCE,
	a.COD_GL_GROUP, b.DES_GL_GROUP, b.ACCOUNT_CONCEPT, c.COD_PL_ACC, d.DES_PL_ACC, e.COD_BLCE_PROD, f.DES_BLCE_PROD, g.COD_BUSINESS_LINE, h.DES_BUSINESS_LINE, NULL AS FTP , NULL AS FTP_RESULT, NULL AS IND_POOL, 
	NULL AS COD_SEGMENT, NULL AS DES_SEGMENT 
	FROM 
		(SELECT * FROM MIS_TMP_TAXES_CONC 
		WHERE EOPBAL_CAP <> 0 OR EOPBAL_INT <> 0 OR PL <> 0) a 
	LEFT JOIN 
		(SELECT DISTINCT COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT FROM MIS_PAR_CAT_CAF) b
		ON a.COD_GL_GROUP = b.COD_GL_GROUP 
	LEFT JOIN 
		MIS_PAR_REL_PL_ACC c 
		ON a.COD_ENTITY = c.COD_ENTITY AND a.COD_GL_GROUP = c.COD_GL_GROUP AND c.COD_CURRENCY='HNL'
    LEFT JOIN 
        MIS_PAR_CAT_PL d
        ON c.COD_PL_ACC = d.COD_PL_ACC
	LEFT JOIN 
	    MIS_PAR_REL_BP_ACC e 
		ON a.COD_ENTITY = e.COD_ENTITY AND a.COD_GL_GROUP = e.COD_GL_GROUP AND e.COD_CURRENCY='HNL'
	LEFT JOIN
		MIS_PAR_CAT_BP f
		ON e.COD_BLCE_PROD = f.COD_BLCE_PROD
	LEFT JOIN 
		MIS_PAR_REL_BL_ACC g
		ON a.COD_ENTITY = g.COD_ENTITY AND a.COD_GL_GROUP = g.COD_GL_GROUP	
	LEFT JOIN 
		MIS_PAR_CAT_BL h
		ON g.COD_BUSINESS_LINE = h.COD_BUSINESS_LINE
	WHERE a.DATA_DATE='${var:periodo}';

------------ Inserción de registros de impuestos -----------
INSERT INTO MIS_DWH_RECONCILIATION PARTITION(DATA_DATE, COD_CONT)
	SELECT A.COD_ACCO_CENT, NULL AS COD_OFFI, NULL AS COD_NAR, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, A.COD_INFO_SOURCE, A.COD_GL_GROUP, 
	A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.FTP, A.FTP_RESULT, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, NULL AS EXP_TYPE, NULL AS EXP_FAMILY, 
	A.DATA_DATE, A.COD_CONT
	FROM MIS_TMP_DWH_OPERACIONAL_RPT_TAXES A;

------------ Inserción de registros conciliados -----------
INSERT INTO MIS_DWH_RECONCILIATION PARTITION(DATA_DATE, COD_CONT)
	SELECT A.COD_ACCO_CENT, NULL AS COD_OFFI, NULL AS COD_NAR, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, A.EOPBAL_CAP, A.EOPBAL_INT, A.AVGBAL_CAP, A.AVGBAL_INT, A.PL, A.COD_INFO_SOURCE, A.COD_GL_GROUP, 
	A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.FTP, A.FTP_RESULT, A.IND_POOL, A.COD_SEGMENT, A.DES_SEGMENT, NULL AS EXP_TYPE, NULL AS EXP_FAMILY, 
	A.DATA_DATE, A.COD_CONT
	FROM MIS_TMP_TAXES_CONC_AUX A
	WHERE a.COD_PL_ACC IS NOT NULL AND a.COD_BLCE_PROD IS NOT NULL;

------------ Inserción de contrapartida de la contabilidad de impuestos -----------
INSERT INTO MIS_DWH_RECONCILIATION PARTITION(DATA_DATE, COD_CONT)
SELECT COD_ACCO_CENT, NULL AS COD_OFFI, NULL AS COD_NAR, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, CAST(-1*PL AS DECIMAL(30,10)) AS PL, COD_INFO_SOURCE, COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP, FTP_RESULT, IND_POOL, COD_SEGMENT, DES_SEGMENT, NULL AS EXP_TYPE, NULL AS EXP_FAMILY, DATA_DATE, 'RCCL' AS COD_CONT 
		FROM MIS_DWH_RECONCILIATION 
		WHERE DATA_DATE='${var:periodo}' AND COD_CONT IN('RCTB','CTBP')
		AND COD_GL_GROUP IN (SELECT DISTINCT COD_GL_GROUP_DESTINY FROM MIS_PAR_TAX_ENG) 
		AND COD_ENTITY IN (SELECT DISTINCT COD_ENTITY FROM MIS_PAR_TAX_ENG)
		;

/*------------ Inserción de registros rechazados -----------
INSERT INTO MIS_REJECTIONS_ALLOCATIONS PARTITION (COD_ENG) 
	SELECT a.DATA_DATE, 'CONCILIACIÓN IMPUESTOS' AS DES_REJ, a.COD_BLCE_STATUS, a.COD_PL_ACC, a.COD_BLCE_PROD, CAST(SUM(a.PL) AS DECIMAL(30,10)) AS PL, 
	CAST(SUM(a.FTP_RESULT) AS DECIMAL(30,10)) AS FTP_RESULT, 'TAX' AS COD_ENG
	FROM MIS_TMP_TAXES_CONC_AUX a
	WHERE a.COD_PL_ACC IS NULL OR a.COD_BLCE_PROD IS NULL
	GROUP BY a.DATA_DATE,  a.COD_BLCE_STATUS, a.COD_PL_ACC, a.COD_BLCE_PROD;*/

--TRUNCATE TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL_RPT_TAXES;
--DROP TABLE IF EXISTS MIS_TMP_DWH_OPERACIONAL_RPT_TAXES;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONC;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC;
--TRUNCATE TABLE IF EXISTS MIS_TMP_TAXES_CONC_AUX;
--DROP TABLE IF EXISTS MIS_TMP_TAXES_CONC_AUX;
--TRUNCATE TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES;
--DROP TABLE IF EXISTS MIS_DWH_RECONCILIATION_TMP_TAXES;