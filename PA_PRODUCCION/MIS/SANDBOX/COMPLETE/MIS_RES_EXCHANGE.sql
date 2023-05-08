------------------------------------------- CONTRAVALORACIÓN Y CARGA FINAL --------------------------------------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--COMPUTE STATS MIS_VAL_CLOSE;

---- Consolidación y preparación de registros que serán agregados a la tabla final 
TRUNCATE TABLE IF EXISTS MIS_DM_BALANCE_RESULT_AUX;

INSERT INTO MIS_DM_BALANCE_RESULT_AUX 
    (COD_CONT, COD_ACCO_CENT, COD_OFFI, COD_NAR, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, EXP_TYPE, EXP_FAMILY, 
    EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, COD_INFO_SOURCE, COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, 
    COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP, FTP_RESULT, IND_POOL, EOPBAL_TOT, AVGBAL_TOT, PL_TOT, COD_SEGMENT, DES_SEGMENT)
PARTITION (DATA_DATE)
SELECT 
    a.COD_CONT, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_NAR, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.EXP_TYPE, a.EXP_FAMILY, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_SEGMENT, a.DES_SEGMENT,
    a.DATA_DATE
FROM MIS_DWH_RECONCILIATION a
WHERE a.DATA_DATE='${var:periodo}';

INSERT INTO MIS_DM_BALANCE_RESULT_AUX 
    (COD_CONT, COD_ACCO_CENT, COD_EXPENSE, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, 
    EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, COD_INFO_SOURCE, COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, 
    COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, FTP, FTP_RESULT, IND_POOL, EOPBAL_TOT, AVGBAL_TOT, PL_TOT,
    COD_SEGMENT, DES_SEGMENT, EXP_TYPE, EXP_FAMILY)
PARTITION (DATA_DATE)
SELECT 
    a.COD_CONT, a.COD_ACCO_CENT, a.COD_EXPENSE, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10)) AS PL_TOT, a.COD_SEGMENT, a.DES_SEGMENT, a.EXP_TYPE, a.EXP_FAMILY, 
    a.DATA_DATE
FROM MIS_DWH_EXPENSES a
WHERE a.DATA_DATE='${var:periodo}';

INSERT INTO MIS_DM_BALANCE_RESULT_AUX 
    (COD_CONT, IDF_CTO, COD_ACCO_CENT, COD_OFFI, COD_BLCE_STATUS, COD_CURRENCY, COD_ENTITY, COD_PRODUCT, COD_SUBPRODUCT, 
    EOPBAL_CAP, EOPBAL_INT, AVGBAL_CAP, AVGBAL_INT, PL, COD_INFO_SOURCE, COD_GL_GROUP, DES_GL_GROUP, ACCOUNT_CONCEPT, 
    COD_PL_ACC, DES_PL_ACC, COD_BLCE_PROD, DES_BLCE_PROD, COD_BUSINESS_LINE, DES_BUSINESS_LINE, COD_VALUE,
    FTP, FTP_RESULT, IND_POOL, EOPBAL_TOT, AVGBAL_TOT, PL_TOT, COD_METHOD_FTP, COD_ACT_TYPE)
PARTITION (DATA_DATE)
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.COD_VALUE,
    CAST(IFNULL(a.FTP1, 0) + IFNULL(a.FTP2, 0) AS decimal(30, 10)) AS FTP, a.FTP_RESULT, a.IND_POOL, 
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_METHOD1, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DWH_ASSETS a
WHERE a.DATA_DATE='${var:periodo}'
UNION ALL
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.COD_VALUE,
    CAST(IFNULL(a.FTP1, 0) + IFNULL(a.FTP2, 0) AS decimal(30, 10)) AS FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_METHOD1, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DWH_LIABILITIES a
WHERE a.DATA_DATE='${var:periodo}'
UNION ALL 
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.COD_VALUE,
    CAST(IFNULL(a.FTP1, 0) + IFNULL(a.FTP2, 0) AS decimal(30, 10)) AS FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_METHOD1, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DWH_PROVISIONS a
WHERE a.DATA_DATE='${var:periodo}'
UNION ALL
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.COD_VALUE,
    CAST(IFNULL(a.FTP1, 0) + IFNULL(a.FTP2, 0) AS decimal(30, 10)) AS FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_METHOD1, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DWH_OFF_BALANCE a
WHERE a.DATA_DATE='${var:periodo}'
UNION ALL
SELECT 
    a.COD_CONT, a.IDF_CTO, a.COD_ACCO_CENT, a.COD_OFFI, a.COD_BLCE_STATUS, a.COD_CURRENCY, a.COD_ENTITY, a.COD_PRODUCT, a.COD_SUBPRODUCT, 
    a.EOPBAL_CAP, a.EOPBAL_INT, a.AVGBAL_CAP, a.AVGBAL_INT, a.PL, a.COD_INFO_SOURCE, a.COD_GL_GROUP, a.DES_GL_GROUP, a.ACCOUNT_CONCEPT, 
    a.COD_PL_ACC, a.DES_PL_ACC, a.COD_BLCE_PROD, a.DES_BLCE_PROD, a.COD_BUSINESS_LINE, a.DES_BUSINESS_LINE, a.COD_VALUE,
    CAST(IFNULL(a.FTP1, 0) + IFNULL(a.FTP2, 0) AS decimal(30, 10)) AS FTP, a.FTP_RESULT, a.IND_POOL,
    CAST(IFNULL(a.EOPBAL_CAP, 0) + IFNULL(a.EOPBAL_INT, 0) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(IFNULL(a.AVGBAL_CAP, 0) + IFNULL(a.AVGBAL_INT, 0) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(IFNULL(a.PL, 0) + IFNULL(a.FTP_RESULT, 0) AS decimal(30, 10))  AS PL_TOT, a.COD_METHOD1, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DWH_FEES a
WHERE a.DATA_DATE='${var:periodo}'; 

INSERT INTO MIS_DM_BALANCE_RESULT_AUX
    (COD_CONT, COD_INFO_SOURCE, IDF_CTO, CARD_NUMBER, COD_CURRENCY, COD_GL_GROUP, DES_GL_GROUP, COD_ENTITY, COD_BLCE_PROD, 
    DES_BLCE_PROD, COD_PL_ACC, DES_PL_ACC,  COD_BUSINESS_LINE,  DES_BUSINESS_LINE, COD_SEGMENT,  EOPBAL_CAP, AVGBAL_CAP,
    PL, INI_AM, DATE_DISB)
    PARTITION (DATA_DATE)
SELECT
    a.CONTENIDO, a.CRITERIO_DE_AJUSTE, CASE WHEN a.CARD_TYPE = 'A' THEN CONCAT(a.CONTRATO, a.CARD_TYPE) ELSE a.CONTRATO END AS IDF_CTO,  
    a.CARD_NUMBER, b.COD_CURRENCY, b.COD_GL_GROUP, b.DES_GL_GROUP, b.COD_ENTITY,
    b.COD_BLCE_PROD, b.DES_BLCE_PROD, b.COD_PL_ACC, b.DES_PL_ACC, b.COD_BUSINESS_LINE, b.DES_BUSINESS_LINE, d.COD_SEGMENT,
    IFNULL(a.IMPORTE_MO,a.IMPORTE_ML) AS EOPBAL_CAP, IFNULL(a.SALDO_MEDIO_MO,a.SALDO_MEDIO_ML) AS AVGBAL_CAP,
    IFNULL(a.PL_MO,a.PL_ML) AS PL, IFNULL(a.INI_AM_MO,a.INI_AM_ML) AS INI_AM, a.DATE_DISB, a.DATA_DATE
FROM MIS_DWH_MANUAL_ADJ_OPER_ADD_CRED_CARD a
LEFT JOIN (SELECT IDF_CTO, COD_CURRENCY, COD_GL_GROUP, DES_GL_GROUP, COD_ENTITY, COD_BLCE_PROD, DES_BLCE_PROD, COD_PL_ACC,
                  DES_PL_ACC, COD_BUSINESS_LINE, DES_BUSINESS_LINE, MIN(COD_INFO_SOURCE)
           FROM MIS_DWH_ASSETS WHERE DATA_DATE = '${var:periodo}' AND COD_VALUE = 'CAP' AND IND_POOL IS NULL
           GROUP BY IDF_CTO, COD_CURRENCY, COD_GL_GROUP, DES_GL_GROUP, COD_ENTITY, COD_BLCE_PROD, DES_BLCE_PROD,
           COD_PL_ACC, DES_PL_ACC, COD_BUSINESS_LINE, DES_BUSINESS_LINE) b
ON a.CONTRATO = b.IDF_CTO
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') c
ON a.CONTRATO = c.IDF_CTO
LEFT JOIN (SELECT * FROM MIS_APR_CLIENT_DT WHERE DATA_DATE = '${var:periodo}') d
ON c.IDF_CLI = d.IDF_CLI
WHERE a.DATA_DATE='${var:periodo}' AND a.CRITERIO_DE_AJUSTE <> 'ADJ_DATE'
;

TRUNCATE TABLE IF EXISTS MIS_PAR_CAT_MANAGER;

INSERT INTO MIS_PAR_CAT_MANAGER
SELECT DISTINCT COD_MANAGER, DES_MANAGER 
FROM MIS_APR_CLIENT_DT 
WHERE DATA_DATE = '${var:periodo}';

--COMPUTE STATS MIS_DM_BALANCE_RESULT_AUX;

----Asignación de información de detalle contrato y cliente
TRUNCATE TABLE IF EXISTS MIS_TMP_DM_BALANCE_DT;
DROP TABLE IF EXISTS MIS_TMP_DM_BALANCE_DT;
CREATE TABLE MIS_TMP_DM_BALANCE_DT AS 
SELECT a.COD_CONT,a.IDF_CTO,a.COD_ACCO_CENT,ISNULL(a.COD_OFFI,b.COD_OFFI) AS COD_OFFI, a.COD_NAR, a.COD_EXPENSE, 
    a.COD_BLCE_STATUS,a.COD_CURRENCY,
    a.COD_ENTITY,a.EXP_TYPE,a.EXP_FAMILY,a.COD_PRODUCT,a.COD_SUBPRODUCT,
    a.EOPBAL_TOT,a.EOPBAL_CAP,a.EOPBAL_INT,a.AVGBAL_TOT,a.AVGBAL_CAP,a.AVGBAL_INT,
    a.PL,a.COD_INFO_SOURCE,a.COD_TYP_FEE,
    c.IDF_CLI,c.TYP_DOC_ID,c.NUM_DOC_ID,c.NOM_NAME,c.NOM_LASTN,c.COD_RES_COUNT,c.COD_TYP_CLNT,c.COD_COND_CLNT, 
    c.COD_ENG,c.VAL_SEX,ISNULL(c.COD_SEGMENT,a.COD_SEGMENT) AS COD_SEGMENT, 
    ISNULL(c.DES_SEGMENT,a.DES_SEGMENT) AS DES_SEGMENT,c.COD_SUBSEGMENT,c.DES_SUBSEGMENT, 
    c.IND_EMPL_GRUP,c.DATE_CLNT_REG,c.DATE_CLNT_WITHDRAW,c.COD_MANAGER,c.DES_MANAGER, 
    c.COD_SECTOR, c.DES_SECTOR, c.RATING_CLI, c.COD_GROUP_EC,
    b.COD_BCA_INT,b.COD_AMRT_MET,b.COD_RATE_TYPE,b.RATE_INT, b.DATE_ORIGIN,b.DATE_LAST_REV,
   b.DATE_PRX_REV,b.EXP_DATE,b.FREQ_INT_PAY,b.COD_UNI_FREQ_INT_PAY,b.FRE_REV_INT,b.COD_UNI_FRE_REV_INT,b.AMRT_TRM,b.COD_UNI_AMRT_TRM,
    CAST(CASE WHEN a.COD_CONT = 'PLZ' THEN
    (CASE WHEN dup.COD_SUBPRODUCT IS NOT NULL THEN b.INI_AM WHEN m.COD_INFO_SOURCE LIKE '%_M' THEN 0 ELSE b.INI_AM END)*-1 ELSE 
    CASE WHEN dup.COD_SUBPRODUCT IS NOT NULL THEN b.INI_AM WHEN m.COD_INFO_SOURCE LIKE '%_M' THEN 0 ELSE b.INI_AM END END AS DECIMAL(30,10)) AS INI_AM,
    b.CUO_AM,b.CREDIT_LIM_AM,b.PREDEF_RATE_IND,b.PREDEF_RATE,b.IND_CHANNEL,b.COD_SELLER,b.DES_SELLER,
    a.PL_TOT,a.AVGBAL_TOT_YTD,a.PL_YTD,a.FTP_RESULT_YTD,a.PL_TOT_YTD,
    a.EOPBAL_TOT_RPT,a.AVGBAL_TOT_RPT,a.PL_TOT_RPT,a.PL_RPT,a.FTP_RESULT_RPT,
    a.AVGBAL_TOT_YTD_RPT,a.PL_YTD_RPT,a.FTP_RESULT_YTD_RPT,a.PL_TOT_YTD_RPT,
    a.COD_GL_GROUP,a.DES_GL_GROUP,a.ACCOUNT_CONCEPT,a.COD_PL_ACC,
    a.DES_PL_ACC,a.COD_BLCE_PROD,a.DES_BLCE_PROD,a.COD_BUSINESS_LINE,a.DES_BUSINESS_LINE, a.COD_VALUE,
    a.FTP,a.FTP_RESULT,a.IND_POOL,CASE WHEN b.COU_CAR_OFF IS NOT NULL THEN b.COU_CAR_OFF ELSE d.COU_CAR_OFF END AS COU_CAR_OFF, 
    CASE WHEN b.COD_CONV IS NOT NULL THEN b.COD_CONV ELSE d.COD_CONV END AS COD_CONV, g.DES_CONV, CASE WHEN c.ID_ECO_GRO IS NOT NULL THEN c.ID_ECO_GRO ELSE f.ID_ECO_GRO END AS ID_ECO_GRO, 
    CASE WHEN c.COU_ECO_GRO IS NOT NULL THEN c.COU_ECO_GRO ELSE f.COU_ECO_GRO END AS COU_ECO_GRO, f.DES_ECO_GRO, CASE WHEN c.IND_MUL_LAT IS NOT NULL THEN c.IND_MUL_LAT ELSE e.IND_MUL_LAT END AS IND_MUL_LAT,
    h.DES_MUL_LAT, b.COD_EXEC_CTO, b.COD_COVID_PORT, CASE WHEN a.COD_INFO_SOURCE = 'ADJ_ADD' OR a.COD_INFO_SOURCE = 'ADJ_IMP' THEN a.DATE_DISB ELSE ISNULL(i.DATE_DISB,b.DATE_ORIGIN) END AS DATE_DISB, c.COD_SECTOR_LOCAL, c.DES_SECTOR_LOCAL, l.COD_SECTOR_REG, l.DES_SECTOR_REG,
    a.COD_METHOD_FTP, j.ID_ECO_GRO_REG, k.DES_ECO_GRO_REG, b.CARD_NUMBER, b.COD_PROG_CARD, b.DES_PROG_CARD, a.COD_ACT_TYPE, 
    a.DATA_DATE
FROM MIS_DM_BALANCE_RESULT_AUX a 
LEFT JOIN (SELECT DISTINCT IDF_CTO, COD_INFO_SOURCE FROM MIS_DM_BALANCE_RESULT_AUX WHERE IND_POOL IS NULL AND COD_VALUE = 'CAP' AND COD_CONT IN ('PREST','CTA','PLZ') AND IDF_CTO IN(
SELECT IDF_CTO FROM MIS_DM_BALANCE_RESULT_AUX 
WHERE IND_POOL IS NULL AND COD_VALUE = 'CAP' AND COD_CONT IN ('PREST','CTA','PLZ') GROUP BY IDF_CTO HAVING COUNT(*)>1)) m
ON a.IDF_CTO = m.IDF_CTO and a.COD_INFO_SOURCE = m.COD_INFO_SOURCE
LEFT JOIN (SELECT a.* FROM MIS_DM_BALANCE_RESULT_AUX a
INNER JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE = '${var:periodo}') dtp
ON a.IDF_CTO = dtp.IDF_CTO AND a.COD_PRODUCT = dtp.COD_PRODUCT AND a.COD_SUBPRODUCT = dtp.COD_SUBPRODUCT
WHERE IND_POOL IS NULL AND COD_VALUE = 'CAP' AND COD_CONT IN ('PREST','CTA','PLZ') AND a.IDF_CTO IN(
SELECT IDF_CTO FROM MIS_DM_BALANCE_RESULT_AUX 
WHERE COD_INFO_SOURCE LIKE '%_M%' AND IND_POOL IS NULL AND COD_VALUE = 'CAP' AND COD_CONT IN ('PREST','CTA','PLZ') 
GROUP BY IDF_CTO HAVING COUNT(*)>1)) dup
ON a.IDF_CTO = dup.IDF_CTO AND a.COD_PRODUCT = dup.COD_PRODUCT AND a.COD_SUBPRODUCT = dup.COD_SUBPRODUCT AND a.COD_GL_GROUP = dup.COD_GL_GROUP AND isnull(a.COD_BLCE_STATUS,'0') = isnull(dup.COD_BLCE_STATUS,'0')
LEFT JOIN (SELECT * FROM MIS_APR_CONTRACT_DT WHERE DATA_DATE='${var:periodo}') b
ON a.DATA_DATE = b.DATA_DATE AND a.IDF_CTO = b.IDF_CTO AND a.COD_ENTITY = b.COD_ENTITY
LEFT JOIN (SELECT * FROM MIS_APR_CLIENT_DT WHERE DATA_DATE='${var:periodo}') c
ON b.DATA_DATE = c.DATA_DATE AND b.IDF_CLI = c.IDF_CLI
LEFT JOIN (SELECT IDF_CLI, IDF_CTO, MAX(ID_ECO_GRO) AS ID_ECO_GRO, MAX(COU_ECO_GRO) AS COU_ECO_GRO, MAX(IND_MUL_LAT) AS IND_MUL_LAT, MAX(COU_CAR_OFF) AS COU_CAR_OFF, MAX(COD_CONV) AS COD_CONV FROM MIS_PAR_REL_REG_DIMENSIONS group by IDF_CLI, IDF_CTO) d
ON a.IDF_CTO = d.IDF_CTO
LEFT JOIN MIS_PAR_REL_REG_DIMENSIONS e
ON b.IDF_CLI = e.IDF_CLI
LEFT JOIN MIS_PAR_CAT_ECO_GRO f
ON c.ID_ECO_GRO = f.ID_ECO_GRO AND c.COU_ECO_GRO = f.COU_ECO_GRO
LEFT JOIN MIS_PAR_CAT_CONV g
ON d.COD_CONV = g.COD_CONV
LEFT JOIN MIS_PAR_CAT_MUL_LAT h
ON e.IND_MUL_LAT = h.IND_MUL_LAT
LEFT JOIN MIS_DWH_MANUAL_ADJ_OPER_ADD_CRED_CARD i
ON i.CARD_NUMBER = b.CARD_NUMBER AND i.CRITERIO_DE_AJUSTE = 'ADJ_DATE'
LEFT JOIN MIS_PAR_REL_ECO_GRO_REG j
ON c.IDF_CLI = j.IDF_CLI
LEFT JOIN MIS_PAR_CAT_ECO_GRO_REG k
ON j.ID_ECO_GRO_REG = k.ID_ECO_GRO_REG
LEFT JOIN MIS_PAR_CAT_SECTOR_ECO l
ON c.COD_SECTOR_LOCAL = l.COD_SECTOR_LOCAL
;

--COMPUTE STATS MIS_TMP_DM_BALANCE_DT;

----Conversión de saldos a Moneda local
TRUNCATE TABLE IF EXISTS MIS_TMP_DM_BALANCE_RESULT;
DROP TABLE IF EXISTS MIS_TMP_DM_BALANCE_RESULT;
CREATE TABLE MIS_TMP_DM_BALANCE_RESULT AS 
SELECT a.COD_CONT,a.IDF_CTO,a.COD_ACCO_CENT,a.COD_OFFI,a.COD_NAR, COD_EXPENSE,a.COD_BLCE_STATUS,a.COD_CURRENCY,a.COD_ENTITY,c.DES_OFFI,a.EXP_TYPE,a.EXP_FAMILY, a.COD_PRODUCT,a.COD_SUBPRODUCT,a.EOPBAL_TOT,a.EOPBAL_CAP,a.EOPBAL_INT,a.AVGBAL_TOT,a.AVGBAL_CAP,a.AVGBAL_INT,a.PL,a.COD_INFO_SOURCE,
a.COD_TYP_FEE,a.IDF_CLI,a.TYP_DOC_ID,a.NUM_DOC_ID,a.NOM_NAME,a.NOM_LASTN,a.COD_RES_COUNT,a.COD_TYP_CLNT,a.COD_COND_CLNT,   a.COD_ENG,a.VAL_SEX,a.COD_SEGMENT,a.DES_SEGMENT,a.COD_SUBSEGMENT,a.DES_SUBSEGMENT,a.IND_EMPL_GRUP,a.DATE_CLNT_REG,a.DATE_CLNT_WITHDRAW,
    a.COD_MANAGER,a.DES_MANAGER,a.COD_SECTOR,a.DES_SECTOR,a.RATING_CLI,a.COD_GROUP_EC,
    a.COD_BCA_INT,a.COD_AMRT_MET,a.COD_RATE_TYPE,a.RATE_INT,a.DATE_ORIGIN,a.DATE_LAST_REV,    a.DATE_PRX_REV,a.EXP_DATE,a.FREQ_INT_PAY,a.COD_UNI_FREQ_INT_PAY,a.FRE_REV_INT,a.COD_UNI_FRE_REV_INT,a.AMRT_TRM,a.COD_UNI_AMRT_TRM,
    a.INI_AM,a.CUO_AM,a.CREDIT_LIM_AM,a.PREDEF_RATE_IND,a.PREDEF_RATE,a.IND_CHANNEL,a.COD_SELLER,a.DES_SELLER,
    a.PL_TOT,a.AVGBAL_TOT_YTD,a.PL_YTD,a.FTP_RESULT_YTD,a.PL_TOT_YTD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.EOPBAL_TOT / b.EXCH_RATE, 0) 
        ELSE IFNULL(a.EOPBAL_TOT * b.EXCH_RATE, 0) END AS decimal(30, 10)) AS EOPBAL_TOT_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.AVGBAL_TOT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.AVGBAL_TOT * b.EXCH_RATE,0) END AS decimal(30, 10)) AS AVGBAL_TOT_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL_TOT  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL_TOT  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_TOT_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.FTP_RESULT  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.FTP_RESULT  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS FTP_RESULT_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.AVGBAL_TOT_YTD / b.EXCH_RATE,0) 
        ELSE IFNULL(a.AVGBAL_TOT_YTD * b.EXCH_RATE,0) END AS decimal(30, 10)) AS AVGBAL_TOT_YTD_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL_YTD  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL_YTD  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_YTD_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.FTP_RESULT_YTD  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.FTP_RESULT_YTD  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS FTP_RESULT_YTD_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL_TOT_YTD  / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL_TOT_YTD  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_TOT_YTD_RPT,
    a.COD_GL_GROUP,a.DES_GL_GROUP,a.ACCOUNT_CONCEPT,a.COD_PL_ACC,
    a.DES_PL_ACC,a.COD_BLCE_PROD,a.DES_BLCE_PROD,a.COD_BUSINESS_LINE,a.DES_BUSINESS_LINE,a.COD_VALUE,
    a.FTP,a.FTP_RESULT,a.IND_POOL,a.COU_CAR_OFF, a.COD_CONV, a.DES_CONV, a.ID_ECO_GRO, a.COU_ECO_GRO, a.DES_ECO_GRO, a.IND_MUL_LAT, a.DES_MUL_LAT, a.COD_EXEC_CTO, a.COD_COVID_PORT, a.DATE_DISB, 
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL_TOT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL_TOT  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_TOT_USD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.PL_TOT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.PL_TOT  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS PL_TOT_REG,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.EOPBAL_TOT / b.EXCH_RATE, 0) 
        ELSE IFNULL(a.EOPBAL_TOT * b.EXCH_RATE, 0) END AS decimal(30, 10)) AS EOPBAL_TOT_USD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.EOPBAL_TOT / b.EXCH_RATE, 0) 
        ELSE IFNULL(a.EOPBAL_TOT * b.EXCH_RATE, 0) END AS decimal(30, 10)) AS EOPBAL_TOT_REG,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.AVGBAL_TOT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.AVGBAL_TOT * b.EXCH_RATE,0) END AS decimal(30, 10)) AS AVGBAL_TOT_USD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.AVGBAL_TOT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.AVGBAL_TOT * b.EXCH_RATE,0) END AS decimal(30, 10)) AS AVGBAL_TOT_REG,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.INI_AM / b.EXCH_RATE,0) 
        ELSE IFNULL(a.INI_AM  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS INI_AM_RPT,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.FTP_RESULT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.FTP_RESULT  * b.EXCH_RATE,0) END AS decimal(30, 6)) AS FTP_RESULT_USD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.FTP_RESULT / b.EXCH_RATE,0) 
        ELSE IFNULL(a.FTP_RESULT  * b.EXCH_RATE,0) END AS decimal(30, 6)) AS FTP_RESULT_REG,
a.COD_SECTOR_LOCAL, a.DES_SECTOR_LOCAL, a.COD_SECTOR_REG, a.DES_SECTOR_REG, a.COD_METHOD_FTP, a.ID_ECO_GRO_REG, a.DES_ECO_GRO_REG, a.CARD_NUMBER, a.COD_PROG_CARD, a.DES_PROG_CARD, a.COD_ACT_TYPE, 
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.INI_AM / b.EXCH_RATE,0) 
        ELSE IFNULL(a.INI_AM  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS INI_AM_USD,
    CAST(CASE WHEN a.COD_CURRENCY='COP' THEN IFNULL(a.INI_AM / b.EXCH_RATE,0) 
        ELSE IFNULL(a.INI_AM  * b.EXCH_RATE,0) END AS decimal(30, 10)) AS INI_AM_REG,
a.DATA_DATE
FROM MIS_TMP_DM_BALANCE_DT a 
LEFT JOIN MIS_PAR_EXCH_RATE b   
ON a.DATA_DATE=b.DATA_DATE AND b.COD_CONT = 'TC_FOTO' AND a.COD_ENTITY=b.COD_ENTITY AND a.COD_CURRENCY=b.COD_CURRENCY
LEFT JOIN MIS_PAR_REL_OFFI c
ON a.COD_OFFI = c.COD_OFFI;


ALTER TABLE MIS_DM_BALANCE_RESULT
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

--COMPUTE STATS MIS_TMP_DM_BALANCE_RESULT;
--COMPUTE STATS MIS_DM_BALANCE_RESULT;

----Carga de contratos y resultados en tabla final
INSERT INTO MIS_DM_BALANCE_RESULT
(COD_CONT,IDF_CTO,COD_ACCO_CENT,COD_OFFI,COD_NAR,COD_EXPENSE,COD_BLCE_STATUS,COD_CURRENCY,COD_ENTITY,COD_PRODUCT,COD_SUBPRODUCT,
COD_INFO_SOURCE,COD_TYP_FEE,IDF_CLI,TYP_DOC_ID,NUM_DOC_ID,NOM_NAME,NOM_LASTN,COD_RES_COUNT,COD_TYP_CLNT,COD_COND_CLNT,COD_ENG,VAL_SEX,COD_SEGMENT,DES_SEGMENT,COD_SUBSEGMENT,DES_SUBSEGMENT,IND_EMPL_GRUP,
DATE_CLNT_REG,DATE_CLNT_WITHDRAW,COD_MANAGER,DES_MANAGER,COD_SECTOR,DES_SECTOR,RATING_CLI,COD_GROUP_EC,COD_BCA_INT,COD_AMRT_MET,COD_RATE_TYPE,RATE_INT,DATE_ORIGIN,DATE_LAST_REV,DATE_PRX_REV,EXP_DATE,
FREQ_INT_PAY,COD_UNI_FREQ_INT_PAY,FRE_REV_INT,COD_UNI_FRE_REV_INT,AMRT_TRM,COD_UNI_AMRT_TRM,INI_AM,CUO_AM,CREDIT_LIM_AM,PREDEF_RATE_IND,PREDEF_RATE,IND_CHANNEL,COD_SELLER,DES_SELLER,
COD_GL_GROUP,DES_GL_GROUP,ACCOUNT_CONCEPT,COD_PL_ACC,DES_PL_ACC,COD_BLCE_PROD,DES_BLCE_PROD,COD_BUSINESS_LINE,DES_BUSINESS_LINE,IND_POOL,
EOPBAL_TOT,EOPBAL_CAP,EOPBAL_INT,AVGBAL_TOT,AVGBAL_CAP,AVGBAL_INT,PL,FTP,FTP_RESULT,
PL_TOT,EOPBAL_TOT_RPT,AVGBAL_TOT_RPT,PL_TOT_RPT,PL_RPT,FTP_RESULT_RPT,AVGBAL_TOT_YTD,PL_YTD,FTP_RESULT_YTD,PL_TOT_YTD,AVGBAL_TOT_YTD_RPT,PL_YTD_RPT,FTP_RESULT_YTD_RPT,PL_TOT_YTD_RPT,DES_OFFI,EXP_TYPE,
EXP_FAMILY,COD_VALUE, COU_CAR_OFF, COD_CONV, DES_CONV, ID_ECO_GRO, COU_ECO_GRO, DES_ECO_GRO, IND_MUL_LAT, DES_MUL_LAT, COD_EXEC_CTO, COD_COVID_PORT, DATE_DISB, PL_TOT_USD, PL_TOT_REG, EOPBAL_TOT_USD, EOPBAL_TOT_REG, AVGBAL_TOT_USD, AVGBAL_TOT_REG, INI_AM_RPT, FTP_RESULT_USD, FTP_RESULT_REG, COD_SECTOR_LOCAL, DES_SECTOR_LOCAL, COD_SECTOR_REG, DES_SECTOR_REG, COD_METHOD_FTP, ID_ECO_GRO_REG, DES_ECO_GRO_REG, CARD_NUMBER, COD_PROG_CARD, DES_PROG_CARD, COD_ACT_TYPE, INI_AM_USD, INI_AM_REG)
PARTITION (DATA_DATE)  
SELECT 
    X.COD_CONT, X.IDF_CTO, X.COD_ACCO_CENT, X.COD_OFFI, X.COD_NAR, X.COD_EXPENSE, X.COD_BLCE_STATUS, X.COD_CURRENCY, X.COD_ENTITY, 
    X.COD_PRODUCT, X.COD_SUBPRODUCT, X.COD_INFO_SOURCE, X.COD_TYP_FEE, X.IDF_CLI, X.TYP_DOC_ID, 
    X.NUM_DOC_ID, X.NOM_NAME, X.NOM_LASTN, X.COD_RES_COUNT, X.COD_TYP_CLNT, X.COD_COND_CLNT, X.COD_ENG, 
    X.VAL_SEX, X.COD_SEGMENT, X.DES_SEGMENT, X.COD_SUBSEGMENT, X.DES_SUBSEGMENT, X.IND_EMPL_GRUP, X.DATE_CLNT_REG, X.DATE_CLNT_WITHDRAW, 
    X.COD_MANAGER, X.DES_MANAGER, X.COD_SECTOR, X.DES_SECTOR, X.RATING_CLI, X.COD_GROUP_EC, 
    X.COD_BCA_INT, X.COD_AMRT_MET, X.COD_RATE_TYPE, 
    CAST(ROUND(X.RATE_INT, 10) AS decimal(30, 10)) AS RATE_INT, X.DATE_ORIGIN, X.DATE_LAST_REV, X.DATE_PRX_REV, X.EXP_DATE, 
    CAST(ROUND(X.FREQ_INT_PAY, 10) AS decimal(30, 10)) AS FREQ_INT_PAY, X.COD_UNI_FREQ_INT_PAY, CAST(ROUND(X.FRE_REV_INT, 10) AS decimal(30, 10)) AS FRE_REV_INT, X.COD_UNI_FRE_REV_INT, 
    CAST(ROUND(X.AMRT_TRM, 10) AS decimal(30, 10)) AS AMRT_TRM, X.COD_UNI_AMRT_TRM, CAST(ROUND(X.INI_AM, 10) AS decimal(30, 10)) AS INI_AM, CAST(ROUND(X.CUO_AM, 10) AS decimal(30, 10)) AS CUO_AM, 
    CAST(ROUND(X.CREDIT_LIM_AM, 10) AS decimal(30, 10)) AS CREDIT_LIM_AM, X.PREDEF_RATE_IND, CAST(ROUND(X.PREDEF_RATE, 10) AS decimal(30, 10)) AS PREDEF_RATE, X.IND_CHANNEL, X.COD_SELLER, X.DES_SELLER,
    X.COD_GL_GROUP, X.DES_GL_GROUP, X.ACCOUNT_CONCEPT, X.COD_PL_ACC, X.DES_PL_ACC, X.COD_BLCE_PROD, X.DES_BLCE_PROD, 
    X.COD_BUSINESS_LINE, X.DES_BUSINESS_LINE, X.IND_POOL, CAST(ROUND(SUM(X.EOPBAL_TOT), 10) AS decimal(30, 10)) AS EOPBAL_TOT, 
    CAST(ROUND(SUM(X.EOPBAL_CAP), 10) AS decimal(30, 10)) AS EOPBAL_CAP, CAST(ROUND(SUM(X.EOPBAL_INT), 10) AS decimal(30, 10)) AS EOPBAL_INT, CAST(ROUND(SUM(X.AVGBAL_TOT), 10) AS decimal(30, 10)) AS AVGBAL_TOT, 
    CAST(ROUND(SUM(X.AVGBAL_CAP), 10) AS decimal(30, 10)) AS AVGBAL_CAP, CAST(ROUND(SUM(X.AVGBAL_INT), 10) AS decimal(30, 10)) AS AVGBAL_INT, CAST(ROUND(SUM(X.PL), 10) AS decimal(30, 10)) AS PL, 
    CAST(SUM(X.FTP) AS decimal(30, 10)) AS FTP, CAST(ROUND(SUM(X.FTP_RESULT), 10) AS decimal(30, 10)) AS FTP_RESULT, CAST(ROUND(SUM(X.PL_TOT), 10) AS decimal(30, 10)) AS PL_TOT, 
    CAST(ROUND(SUM(X.EOPBAL_TOT_RPT), 10) AS decimal(30, 10)) AS EOPBAL_TOT_RPT, CAST(ROUND(SUM(X.AVGBAL_TOT_RPT), 10) AS decimal(30, 10)) AS AVGBAL_TOT_RPT, 
    CAST(ROUND(SUM(X.PL_TOT_RPT), 10) AS decimal(30, 10)) AS PL_TOT_RPT, CAST(ROUND(SUM(X.PL_RPT), 10) AS decimal(30, 10)) AS PL_RPT, CAST(ROUND(SUM(X.FTP_RESULT_RPT), 10) AS decimal(30, 10)) AS FTP_RESULT_RPT, 
    NULL AS AVGBAL_TOT_YTD, 
    NULL AS PL_YTD,
    NULL AS FTP_RESULT_YTD,
    NULL AS PL_TOT_YTD, 
    NULL AS AVGBAL_TOT_YTD_RPT, 
    NULL AS PL_YTD_RPT, 
    NULL AS FTP_RESULT_YTD_RPT,
    NULL AS PL_TOT_YTD_RPT, 
    X.DES_OFFI,X.EXP_TYPE,X.EXP_FAMILY,X.COD_VALUE, X.COU_CAR_OFF, X.COD_CONV, X.DES_CONV, X.ID_ECO_GRO, X.COU_ECO_GRO, X.DES_ECO_GRO, 
    X.IND_MUL_LAT, X.DES_MUL_LAT, X.COD_EXEC_CTO, X.COD_COVID_PORT, X.DATE_DISB,  
    CAST(ROUND(SUM(X.PL_TOT_USD), 10) AS DECIMAL(30, 10)) AS PL_TOT_USD, 
    CAST(ROUND(SUM(X.PL_TOT_REG), 10) AS DECIMAL(30, 10)) AS PL_TOT_REG, 
    CAST(ROUND(SUM(X.EOPBAL_TOT_USD), 10) AS DECIMAL(30, 10)) AS EOPBAL_TOT_USD, 
    CAST(ROUND(SUM(X.EOPBAL_TOT_REG), 10) AS DECIMAL(30, 10)) AS EOPBAL_TOT_REG,     
    CAST(ROUND(SUM(X.AVGBAL_TOT_USD), 10) AS DECIMAL(30, 10)) AS AVGBAL_TOT_USD, 
    CAST(ROUND(SUM(X.AVGBAL_TOT_REG), 10) AS DECIMAL(30, 10)) AS AVGBAL_TOT_REG, 
    CAST(ROUND(SUM(X.INI_AM_RPT), 10) AS DECIMAL(30, 10)) AS INI_AM_RPT, 
    CAST(ROUND(SUM(X.FTP_RESULT_USD),10) AS DECIMAL(30, 10)) AS FTP_RESULT_USD,
    CAST(ROUND(SUM(X.FTP_RESULT_REG),10) AS DECIMAL(30, 10)) AS FTP_RESULT_REG,
X.COD_SECTOR_LOCAL, X.DES_SECTOR_LOCAL, X.COD_SECTOR_REG, X.DES_SECTOR_REG, X.COD_METHOD_FTP, X.ID_ECO_GRO_REG, X.DES_ECO_GRO_REG, X.CARD_NUMBER, X.COD_PROG_CARD, X.DES_PROG_CARD, X.COD_ACT_TYPE, 
    CAST(ROUND(X.INI_AM_USD, 10) AS decimal(30, 10)) AS INI_AM_USD,
    CAST(ROUND(X.INI_AM_REG, 10) AS decimal(30, 10)) AS INI_AM_REG, X.DATA_DATE 
FROM
    (SELECT 
        A.COD_CONT, A.IDF_CTO, A.COD_ACCO_CENT, A.COD_OFFI, A.COD_NAR, A.COD_EXPENSE, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, 
        A.COD_PRODUCT, A.COD_SUBPRODUCT, A.COD_INFO_SOURCE, A.COD_TYP_FEE, A.IDF_CLI, A.TYP_DOC_ID, 
        A.NUM_DOC_ID, A.NOM_NAME, A.NOM_LASTN, A.COD_RES_COUNT, A.COD_TYP_CLNT, A.COD_COND_CLNT, A.COD_ENG,
        A.VAL_SEX, A.COD_SEGMENT, A.DES_SEGMENT, A.COD_SUBSEGMENT, A.DES_SUBSEGMENT, A.IND_EMPL_GRUP, A.DATE_CLNT_REG, A.DATE_CLNT_WITHDRAW,
        A.COD_MANAGER, A.DES_MANAGER, A.COD_SECTOR, A.DES_SECTOR, A.RATING_CLI, A.COD_GROUP_EC,
        A.COD_BCA_INT, A.COD_AMRT_MET, A.COD_RATE_TYPE, A.RATE_INT, A.DATE_ORIGIN, 
        A.DATE_LAST_REV, A.DATE_PRX_REV, A.EXP_DATE, A.FREQ_INT_PAY, A.COD_UNI_FREQ_INT_PAY, A.FRE_REV_INT,
        A.COD_UNI_FRE_REV_INT, A.AMRT_TRM, A.COD_UNI_AMRT_TRM, A.INI_AM, A.CUO_AM, A.CREDIT_LIM_AM, 
        A.PREDEF_RATE_IND, A.PREDEF_RATE, A.IND_CHANNEL, A.COD_SELLER, A.DES_SELLER,A.COD_GL_GROUP, A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, 
        A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE,
        A.IND_POOL, SUM(A.EOPBAL_TOT) AS EOPBAL_TOT, SUM(A.EOPBAL_CAP) AS EOPBAL_CAP, SUM(A.EOPBAL_INT) AS EOPBAL_INT, SUM(A.AVGBAL_TOT) AS AVGBAL_TOT, 
        SUM(A.AVGBAL_CAP) AS AVGBAL_CAP, SUM(A.AVGBAL_INT) AS AVGBAL_INT, SUM(A.PL) AS PL, SUM(A.FTP) AS FTP, SUM(A.FTP_RESULT) AS FTP_RESULT, SUM(A.PL_TOT) AS PL_TOT, 
        SUM(A.EOPBAL_TOT_RPT) AS EOPBAL_TOT_RPT, SUM(A.AVGBAL_TOT_RPT) AS AVGBAL_TOT_RPT, SUM(A.PL_TOT_RPT) AS PL_TOT_RPT, SUM(A.PL_RPT) AS PL_RPT, SUM(A.FTP_RESULT_RPT) AS FTP_RESULT_RPT,
        CASE 
        WHEN MONTH(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 AND DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 THEN SUM(A.AVGBAL_TOT)
        ELSE SUM(A.AVGBAL_TOT * DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')))
        END AS AVGBAL_TOT_YTD,
        SUM(A.PL) AS PL_YTD, 
        CASE 
        WHEN MONTH(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 AND DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 THEN SUM(A.FTP_RESULT) 
        ELSE SUM(A.FTP_RESULT_YTD) 
        END AS FTP_RESULT_YTD, 
        SUM(A.PL_TOT) AS PL_TOT_YTD, 
        CASE 
        WHEN MONTH(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 AND DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')) = 1 THEN SUM(A.AVGBAL_TOT_RPT) 
        ELSE SUM(A.AVGBAL_TOT_RPT * DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')))  
        END AS AVGBAL_TOT_YTD_RPT, 
        SUM(A.PL_RPT) AS PL_YTD_RPT,
        SUM(A.FTP_RESULT_RPT)  AS FTP_RESULT_YTD_RPT, 
        SUM(A.PL_TOT_RPT) AS PL_TOT_YTD_RPT, 
        A.DES_OFFI, A.EXP_TYPE, A.EXP_FAMILY,A.COD_VALUE, A.COU_CAR_OFF, A.COD_CONV, A.DES_CONV, A.ID_ECO_GRO, A.COU_ECO_GRO, 
        A.DES_ECO_GRO, A.IND_MUL_LAT, A.DES_MUL_LAT, A.COD_EXEC_CTO, A.COD_COVID_PORT, A.DATE_DISB, 
        SUM(A.PL_TOT_USD) AS PL_TOT_USD, SUM(A.PL_TOT_REG) AS PL_TOT_REG, 
        IFNULL(SUM(A.EOPBAL_TOT_RPT / b.EXCH_RATE), 0) AS EOPBAL_TOT_USD, 
        SUM(A.EOPBAL_TOT_REG) AS EOPBAL_TOT_REG, 
        IFNULL(SUM(A.AVGBAL_TOT_RPT / b.EXCH_RATE), 0) AS AVGBAL_TOT_USD, 
        SUM(A.AVGBAL_TOT_REG) AS AVGBAL_TOT_REG, A.INI_AM_RPT, 
        SUM(A.FTP_RESULT_USD) AS FTP_RESULT_USD, SUM(A.FTP_RESULT_REG) AS FTP_RESULT_REG,
        A.COD_SECTOR_LOCAL, A.DES_SECTOR_LOCAL, A.COD_SECTOR_REG,
        A.DES_SECTOR_REG, A.COD_METHOD_FTP, A.ID_ECO_GRO_REG, A.DES_ECO_GRO_REG, A.CARD_NUMBER, A.COD_PROG_CARD, A.DES_PROG_CARD, 
        A.COD_ACT_TYPE, A.INI_AM_USD, A.INI_AM_REG, A.DATA_DATE
    FROM MIS_TMP_DM_BALANCE_RESULT AS A 
    LEFT JOIN MIS_PAR_EXCH_RATE b
    ON A.DATA_DATE=b.DATA_DATE AND b.COD_CONT = 'TC_FOTO' AND b.COD_CURRENCY = 'USD' AND A.COD_ENTITY=b.COD_ENTITY
    GROUP BY A.DATA_DATE, A.COD_CONT, A.IDF_CTO, A.COD_ACCO_CENT, A.COD_OFFI, A.COD_NAR, A.COD_EXPENSE, A.COD_BLCE_STATUS, A.COD_CURRENCY, A.COD_ENTITY, A.COD_PRODUCT, A.COD_SUBPRODUCT, A.COD_INFO_SOURCE, A.COD_TYP_FEE, A.IDF_CLI,
    A.TYP_DOC_ID, A.NUM_DOC_ID, A.NOM_NAME, A.NOM_LASTN, A.COD_RES_COUNT, A.COD_TYP_CLNT, A.COD_COND_CLNT, A.COD_ENG, A.VAL_SEX, A.COD_SEGMENT, A.DES_SEGMENT, A.COD_SUBSEGMENT, A.DES_SUBSEGMENT, A.IND_EMPL_GRUP, 
    A.DATE_CLNT_REG, A.DATE_CLNT_WITHDRAW, A.COD_MANAGER, A.DES_MANAGER, A.COD_SECTOR, A.DES_SECTOR, A.RATING_CLI, A.COD_GROUP_EC, A.COD_BCA_INT, A.COD_AMRT_MET, A.COD_RATE_TYPE, A.RATE_INT, A.DATE_ORIGIN, A.DATE_LAST_REV, 
    A.DATE_PRX_REV, A.EXP_DATE, A.FREQ_INT_PAY, A.COD_UNI_FREQ_INT_PAY, A.FRE_REV_INT, A.COD_UNI_FRE_REV_INT, A.AMRT_TRM, A.COD_UNI_AMRT_TRM, A.INI_AM, A.CUO_AM, A.CREDIT_LIM_AM, A.PREDEF_RATE_IND, A.PREDEF_RATE, 
    A.IND_CHANNEL, A.COD_SELLER, A.DES_SELLER, A.COD_GL_GROUP, A.DES_GL_GROUP, A.ACCOUNT_CONCEPT, A.COD_PL_ACC, A.DES_PL_ACC, A.COD_BLCE_PROD, A.DES_BLCE_PROD, A.COD_BUSINESS_LINE, A.DES_BUSINESS_LINE, A.IND_POOL,
    A.DES_OFFI, A.EXP_TYPE, A.EXP_FAMILY, A.COD_VALUE, A.COU_CAR_OFF, A.COD_CONV, A.DES_CONV, A.ID_ECO_GRO, A.COU_ECO_GRO, A.DES_ECO_GRO, A.IND_MUL_LAT, A.DES_MUL_LAT, A.COD_EXEC_CTO, A.COD_COVID_PORT, A.DATE_DISB, A.INI_AM_RPT,
    A.COD_SECTOR_LOCAL, A.DES_SECTOR_LOCAL, A.COD_SECTOR_REG,A.DES_SECTOR_REG, A.COD_METHOD_FTP, A.ID_ECO_GRO_REG, A.DES_ECO_GRO_REG, A.CARD_NUMBER, A.COD_PROG_CARD, A.DES_PROG_CARD, A.COD_ACT_TYPE, A.INI_AM_USD, A.INI_AM_REG
) X 
GROUP BY X.DATA_DATE, X.COD_CONT, X.IDF_CTO, X.COD_ACCO_CENT, X.COD_OFFI, X.COD_NAR, X.COD_EXPENSE, X.COD_BLCE_STATUS, X.COD_CURRENCY, X.COD_ENTITY, X.COD_PRODUCT, X.COD_SUBPRODUCT, X.COD_INFO_SOURCE, X.COD_TYP_FEE, X.IDF_CLI, 
X.TYP_DOC_ID, X.NUM_DOC_ID, X.NOM_NAME, X.NOM_LASTN, X.COD_RES_COUNT, X.COD_TYP_CLNT, X.COD_COND_CLNT, X.COD_ENG, X.VAL_SEX, X.COD_SEGMENT, X.DES_SEGMENT, X.COD_SUBSEGMENT, X.DES_SUBSEGMENT, X.IND_EMPL_GRUP, X.DATE_CLNT_REG, 
X.DATE_CLNT_WITHDRAW, X.COD_MANAGER, X.DES_MANAGER, X.COD_SECTOR, X.DES_SECTOR, X.RATING_CLI, X.COD_GROUP_EC, X.COD_BCA_INT, X.COD_AMRT_MET, X.COD_RATE_TYPE, X.RATE_INT, X.DATE_ORIGIN, X.DATE_LAST_REV, 
X.DATE_PRX_REV, X.EXP_DATE, X.FREQ_INT_PAY, X.COD_UNI_FREQ_INT_PAY, X.FRE_REV_INT, X.COD_UNI_FRE_REV_INT, X.AMRT_TRM, X.COD_UNI_AMRT_TRM, X.INI_AM, X.CUO_AM, X.CREDIT_LIM_AM, X.PREDEF_RATE_IND, X.PREDEF_RATE, 
X.IND_CHANNEL, X.COD_SELLER, X.DES_SELLER, X.COD_GL_GROUP, X.DES_GL_GROUP, X.ACCOUNT_CONCEPT, X.COD_PL_ACC, X.DES_PL_ACC, X.COD_BLCE_PROD, X.DES_BLCE_PROD, X.COD_BUSINESS_LINE, X.DES_BUSINESS_LINE, X.IND_POOL,
X.DES_OFFI, X.EXP_TYPE, X.EXP_FAMILY, X.COD_VALUE, X.COU_CAR_OFF, X.COD_CONV, X.DES_CONV, X.ID_ECO_GRO, X.COU_ECO_GRO, X.DES_ECO_GRO, X.IND_MUL_LAT, X.DES_MUL_LAT, X.COD_EXEC_CTO, X.COD_COVID_PORT, X.DATE_DISB, X.INI_AM_RPT,
X.COD_SECTOR_LOCAL, X.DES_SECTOR_LOCAL, X.COD_SECTOR_REG,X.DES_SECTOR_REG, X.COD_METHOD_FTP, X.ID_ECO_GRO_REG, X.DES_ECO_GRO_REG, X.CARD_NUMBER, X.COD_PROG_CARD, X.DES_PROG_CARD, X.COD_ACT_TYPE, X.INI_AM_USD, X.INI_AM_REG;

--TRUNCATE TABLE IF EXISTS MIS_TMP_DM_BALANCE_RESULT;
--DROP TABLE IF EXISTS MIS_TMP_DM_BALANCE_RESULT;
--TRUNCATE TABLE IF EXISTS MIS_TMP_DM_BALANCE_DT;
--DROP TABLE IF EXISTS MIS_TMP_DM_BALANCE_DT;


------------- Validaciones de cierre -------------

--- Creación de tabla temporal para almacenar resultados ---
DROP TABLE IF EXISTS MIS_TMP_CLOSING_VALIDATION;
CREATE TABLE MIS_TMP_CLOSING_VALIDATION
(COD_VALID STRING, VALIDATION_FIELD STRING, COD_CONT STRING);


DROP TABLE IF EXISTS MIS_TMP_COD_VALIDATION;
CREATE TABLE MIS_TMP_COD_VALIDATION
(COD_VALID STRING, DESCRIPTION STRING);

--- Inserción de códigos de validación de cierre ---
INSERT INTO MIS_TMP_COD_VALIDATION VALUES ('001', 'Combinación Producto de Balance, Estado de Balance y Moneda sin Parametrizar en Tabla de Métodos de TTI');
INSERT INTO MIS_TMP_COD_VALIDATION VALUES ('002', 'Producto de Balance sin Parametrizar en Jerarquía de Producto a Último Nivel');
INSERT INTO MIS_TMP_COD_VALIDATION VALUES ('003', 'Cuenta P&G sin Parametrizar en Jerarquía de Cuenta a Último Nivel');
INSERT INTO MIS_TMP_COD_VALIDATION VALUES ('004', 'Producto de Balance sin Parametrizar en Jerarquía de Producto balance y Linea de negocio a Último Nivel');

-------------------- Verificación de Productos Balance a los que no se calculó TTI --------------------
INSERT INTO MIS_TMP_CLOSING_VALIDATION (COD_VALID, VALIDATION_FIELD, COD_CONT)
SELECT '001' AS COD_VALID, CONCAT(COD_BLCE_PROD, " |", COD_BLCE_STATUS, "| ", COD_CURRENCY, COD_CONT) AS CAMPOS, COD_CONT
FROM 
    (SELECT DISTINCT SALIDA_DM.COD_BLCE_PROD, SALIDA_DM.COD_BLCE_STATUS , SALIDA_DM.COD_CURRENCY, SALIDA_DM.COD_CONT
    FROM
        (SELECT DM.COD_BLCE_PROD, DM.COD_BLCE_STATUS, DM.COD_CURRENCY, DM.COD_CONT
        FROM MIS_DM_BALANCE_RESULT DM
        WHERE DM.DATA_DATE='${var:periodo}' AND DM.IND_POOL IS NULL AND DM.COD_VALUE IN ('CAP','INT') AND DM.COD_CONT NOT IN ('RCCL')) SALIDA_DM
    LEFT JOIN 
        MIS_PAR_TTI_ENG TTI_ENG  
        ON TTI_ENG.COD_BLCE_PROD=SALIDA_DM.COD_BLCE_PROD AND TTI_ENG.COD_BLCE_STATUS=SALIDA_DM.COD_BLCE_STATUS AND TTI_ENG.COD_CURRENCY=SALIDA_DM.COD_CURRENCY
    WHERE TTI_ENG.COD_BLCE_PROD IS NULL) AS VALIDATION_TTI;

-------------------- Verificación de Producto Balance a último Nivel de Jerarquía --------------------
INSERT INTO MIS_TMP_CLOSING_VALIDATION (COD_VALID, VALIDATION_FIELD, COD_CONT)
SELECT '002' AS COD_VALID, VALIDATION_BLCE_PROD.COD_BLCE_PROD, COD_CONT
FROM
    (SELECT DISTINCT(SALIDA_DM.COD_BLCE_PROD) COD_BLCE_PROD, COD_CONT
    FROM
        (SELECT DM.COD_BLCE_PROD, DM.COD_CONT
        FROM MIS_DM_BALANCE_RESULT DM
        WHERE DM.DATA_DATE='${var:periodo}') SALIDA_DM
    LEFT JOIN
        MIS_HIERARCHY_BLCE_PROD PROD
        ON SALIDA_DM.COD_BLCE_PROD = PROD.COD_LEVEL_07
    WHERE PROD.COD_LEVEL_07 IS NULL) AS VALIDATION_BLCE_PROD;

-------------------- Verificación de Cuenta P&G a último Nivel de Jerarquía --------------------
INSERT INTO MIS_TMP_CLOSING_VALIDATION (COD_VALID, VALIDATION_FIELD, COD_CONT)
SELECT '003' AS COD_VALID, VALIDATION_PL_ACC.COD_PL_ACC, COD_CONT
FROM
    (SELECT DISTINCT(SALIDA_DM.COD_PL_ACC) COD_PL_ACC, COD_CONT
    FROM
        (SELECT DM.* 
        FROM MIS_DM_BALANCE_RESULT DM
        WHERE DM.DATA_DATE='${var:periodo}') SALIDA_DM
    LEFT JOIN
        MIS_HIERARCHY_PL_ACC ACC
        ON SALIDA_DM.COD_PL_ACC = ACC.COD_LEVEL_12
    WHERE ACC.COD_LEVEL_12 IS NULL) AS VALIDATION_PL_ACC; 

------------------ Verificación de Producto Balance a último Nivel de Jerarquía de producto de balance y linea de negocio ----------------
INSERT INTO MIS_TMP_CLOSING_VALIDATION (COD_VALID, VALIDATION_FIELD, COD_CONT)
SELECT '004' AS COD_VALID, VALIDATION_BLCE_PROD.COD_BLCE_PROD, COD_CONT
FROM
    (SELECT DISTINCT(SALIDA_DM.COD_BLCE_PROD) COD_BLCE_PROD, COD_CONT
    FROM
        (SELECT DM.COD_BLCE_PROD, DM.COD_CONT 
        FROM MIS_DM_BALANCE_RESULT DM
        WHERE DM.DATA_DATE='${var:periodo}') SALIDA_DM
    LEFT JOIN
        MIS_HIERARCHY_PROD_BL PROD
        ON SALIDA_DM.COD_BLCE_PROD = PROD.COD_LEVEL_02
    WHERE PROD.COD_LEVEL_02 IS NULL) AS VALIDATION_BLCE_PROD;

-------------------- Llenado de la tabla final de validaciones de cierre --------------------

--COMPUTE STATS MIS_TMP_COD_VALIDATION;
--COMPUTE STATS MIS_TMP_CLOSING_VALIDATION;

ALTER TABLE MIS_VAL_CLOSE
DROP IF EXISTS PARTITION (TYPE = 'DIARIA');

INSERT INTO MIS_VAL_CLOSE
PARTITION (TYPE)
SELECT COD.DESCRIPTION, VAL.VALIDATION_FIELD, VAL.COD_CONT,'DIARIA' AS TYPE
FROM MIS_TMP_CLOSING_VALIDATION VAL
INNER JOIN MIS_TMP_COD_VALIDATION COD
ON VAL.COD_VALID = COD.COD_VALID;

--- Eliminación de Tablas Temporales ---
TRUNCATE TABLE IF EXISTS MIS_TMP_COD_VALIDATION;
DROP TABLE IF EXISTS MIS_TMP_COD_VALIDATION;
TRUNCATE TABLE IF EXISTS MIS_TMP_CLOSING_VALIDATION;
DROP TABLE IF EXISTS MIS_TMP_CLOSING_VALIDATION;