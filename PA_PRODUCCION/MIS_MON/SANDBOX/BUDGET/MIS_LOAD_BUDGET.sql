--- Comando que apunta a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Llenado de Tabla de Presupuesto ---
TRUNCATE TABLE MIS_LOAD_BUDGET;
LOAD DATA INPATH '${var:ruta_fuentes_presupuesto}/PRESUPUESTO.csv' INTO TABLE MIS_LOAD_BUDGET;

ALTER TABLE MIS_APR_BUDGET
DROP IF EXISTS PARTITION (YEAR = STRLEFT('${var:periodo}',4), ESCENARIO = ${var:version_ppto});

ALTER TABLE MIS_DM_BUDGET
DROP IF EXISTS PARTITION (YEAR = STRLEFT('${var:periodo}',4), ESCENARIO = ${var:version_ppto});

---Insertar la última versión de la tabla de presupuesto ---
INSERT INTO MIS_APR_BUDGET
PARTITION (YEAR, ESCENARIO)
SELECT FROM_TIMESTAMP(LAST_DAY(TO_TIMESTAMP(A.DATA_DATE, 'yyyyMMdd')), 'yyyyMMdd') AS DATA_DATE,
    A.COD_CONT,
    A.COD_GL_GROUP,
    A.COD_BUSINESS_LINE,
    A.COD_PL_ACC,
    A.COD_BLCE_PROD,
    'USD' AS COD_CURRENCY,
    A.IND_MENSUALIZA,
    STRRIGHT(CONCAT('0', A.COD_ENTITY),2),
    A.COD_SEGMENT,
    A.COD_ACCO_CENT,
    A.COD_EXPENSE,
    A.EXP_FAM,
    A.EXP_TYPE,
    CAST(CASE
        WHEN A.COD_CURRENCY='COP' THEN CAST(A.PL AS decimal(30, 10)) / RT_BAL.EXCH_RATE
        ELSE CAST(A.PL AS decimal(30, 10)) * 1 
        END AS decimal(30, 10)) AS PL, 
    CAST(CASE
        WHEN A.COD_CURRENCY='COP' THEN CAST(A.EOPBAL_CAP AS decimal(30, 10)) / RT_BAL.EXCH_RATE 
        ELSE CAST(A.EOPBAL_CAP AS decimal(30, 10)) * 1
        END AS decimal(30, 10)) AS EOPBAL_CAP, 
    CAST(CASE
        WHEN A.COD_CURRENCY='COP' THEN CAST(A.AVGBAL_CAP AS decimal(30, 10)) / RT_BAL.EXCH_RATE
        ELSE CAST(A.AVGBAL_CAP AS decimal(30, 10)) * 1
        END AS decimal(30, 10)) AS AVGBAL_CAP, 
    A.NUM_CTOS,
    CAST(CASE
        WHEN A.COD_CURRENCY='COP' THEN CAST(A.INI_AM AS decimal(30, 10)) / RT_BAL.EXCH_RATE
        ELSE CAST(A.INI_AM AS decimal(30, 10)) * 1
        END AS decimal(30, 10)) AS INI_AM,
       SUBSTR('${var:periodo}',1,4) AS YEAR,
       A.ESCENARIO
FROM MIS_LOAD_BUDGET AS A
LEFT JOIN MIS_PAR_EXCH_RATE RT_BAL
ON A.DATA_DATE = RT_BAL.DATA_DATE AND RT_BAL.COD_CONT = 'TC_FOTO'  AND A.COD_ENTITY = RT_BAL.COD_ENTITY 
    AND A.COD_CURRENCY = RT_BAL.COD_CURRENCY
WHERE --A.ESCENARIO IS NOT NULL AND 
      NOT EXISTS (SELECT 1
                  FROM MIS_APR_BUDGET
                  WHERE ESCENARIO = A.ESCENARIO AND STRLEFT(DATA_DATE,4) = STRLEFT(A.DATA_DATE,4));


---Tomar la última versión del presupuesto filtrado por la fecha con la que se ejecuta la malla---

INSERT INTO MIS_DM_BUDGET
(DATA_DATE, COD_CONT, COD_GL_GROUP, COD_BUSINESS_LINE, COD_PL_ACC, COD_BLCE_PROD, COD_CURRENCY, 
PL, PL_YTD, EOPBAL_CAP, AVGBAL_CAP, INI_AM, PL_PPTO_USD, PL_PPTO_YTD_USD, EOPBAL_TOT_USD, EOPBAL_TOT_REG, AVGBAL_TOT_USD,AVGBAL_TOT_REG, INI_AM_REG, 
PL_PPTO_REG, PL_PPTO_YTD_REG, COD_ENTITY, COD_SEGMENT, COD_ACCO_CENT, COD_EXPENSE, EXP_FAM, EXP_TYPE, NUM_CTOS)
PARTITION (YEAR, ESCENARIO)
SELECT DM.DATA_DATE, ACT.COD_CONT, DM.COD_GL_GROUP, DM.COD_BUSINESS_LINE, DM.COD_PL_ACC, DM.COD_BLCE_PROD, DM.COD_CURRENCY,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) ELSE CAST((MAX(ISNULL(ACT.PL, 0))-MAX(ISNULL(PRV.PL, 0))) AS DECIMAL(30, 10)) END AS PL,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(SUM(ACU.PL) AS DECIMAL(30, 10)) ELSE CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) END AS PL_YTD,
CAST(ACT.EOPBAL_CAP AS DECIMAL(30, 10)) AS EOPBAL_CAP, CAST(ACT.AVGBAL_CAP AS DECIMAL(30, 10)) AS AVGBAL_CAP,
CAST(ACT.INI_AM AS DECIMAL(30, 10)) AS INI_AM,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) ELSE CAST((MAX(ISNULL(ACT.PL, 0))-MAX(ISNULL(PRV.PL, 0))) AS DECIMAL(30, 10)) END AS PL_PPTO_USD,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(SUM(ACU.PL) AS DECIMAL(30, 10)) ELSE CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) END AS PL_PPTO_YTD_USD,
CAST(ACT.EOPBAL_CAP AS DECIMAL(30, 10)) AS EOPBAL_TOT_USD,
CAST(ACT.EOPBAL_CAP AS DECIMAL(30, 10)) AS EOPBAL_TOT_REG,
CAST(ACT.AVGBAL_CAP AS DECIMAL(30, 10)) AS AVGBAL_TOT_USD,
CAST(ACT.AVGBAL_CAP AS DECIMAL(30, 10)) AS AVGBAL_TOT_REG,
CAST(ACT.INI_AM AS DECIMAL(30, 10)) AS INI_AM_REG,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) ELSE CAST((MAX(ISNULL(ACT.PL, 0))-MAX(ISNULL(PRV.PL, 0))) AS DECIMAL(30, 10)) END AS PL_PPTO_REG,
CASE WHEN DM.IND_MENSUALIZA = 'Y' THEN CAST(SUM(ACU.PL) AS DECIMAL(30, 10)) ELSE CAST(MAX(ACT.PL) AS DECIMAL(30, 10)) END AS PL_PPTO_YTD_REG,
DM.COD_ENTITY, DM.COD_SEGMENT, DM.COD_ACCO_CENT,DM.COD_EXPENSE,DM.EXP_TYPE,DM.EXP_FAM, CAST(ACT.NUM_CTOS AS INT),
SUBSTR('${var:periodo}',1,4) AS YEAR, DM.ESCENARIO
FROM (
    SELECT DT.DATA_DATE, DM.COD_GL_GROUP, DM.COD_BUSINESS_LINE, DM.COD_PL_ACC, DM.COD_BLCE_PROD, 
    DM.COD_CURRENCY, DM.IND_MENSUALIZA, DM.COD_ENTITY, DM.COD_SEGMENT, DM.COD_ACCO_CENT, DM.COD_EXPENSE, DM.EXP_TYPE,
    DM.EXP_FAM, DM.ESCENARIO 
    FROM MIS_APR_BUDGET DM  
    LEFT JOIN (SELECT DISTINCT DATA_DATE FROM MIS_APR_BUDGET) DT
    ON 1=1
    GROUP BY DT.DATA_DATE, DM.COD_GL_GROUP, DM.COD_BUSINESS_LINE, DM.COD_PL_ACC, DM.COD_BLCE_PROD, 
    DM.COD_CURRENCY, DM.IND_MENSUALIZA, DM.COD_ENTITY, DM.COD_SEGMENT, DM.COD_ACCO_CENT, DM.COD_EXPENSE, DM.EXP_TYPE,
    DM.EXP_FAM, DM.ESCENARIO) DM 
LEFT JOIN (
    SELECT ACT.DATA_DATE, ACT.COD_CONT, ACT.COD_GL_GROUP, ACT.COD_BUSINESS_LINE, ACT.COD_PL_ACC, ACT.COD_BLCE_PROD, ACT.COD_CURRENCY, ACT.IND_MENSUALIZA, 
    SUM(ACT.PL) AS PL, SUM(ACT.EOPBAL_CAP) AS EOPBAL_CAP, SUM(ACT.AVGBAL_CAP) AS AVGBAL_CAP, SUM(ACT.INI_AM) AS INI_AM,
    ACT.COD_ENTITY, ACT.COD_SEGMENT, ACT.COD_ACCO_CENT, ACT.COD_EXPENSE, ACT.EXP_TYPE, ACT.EXP_FAM, CAST(SUM(ACT.NUM_CTOS) AS INT) AS NUM_CTOS, ACT.ESCENARIO 
    FROM MIS_APR_BUDGET ACT 
    GROUP BY ACT.DATA_DATE, ACT.COD_CONT, ACT.COD_GL_GROUP, ACT.COD_BUSINESS_LINE, ACT.COD_PL_ACC, ACT.COD_BLCE_PROD, ACT.COD_CURRENCY, ACT.IND_MENSUALIZA, 
    ACT.COD_ENTITY, ACT.COD_SEGMENT, ACT.COD_ACCO_CENT, ACT.COD_EXPENSE, ACT.EXP_TYPE, ACT.EXP_FAM, ACT.ESCENARIO) ACT
ON DM.COD_GL_GROUP = ACT.COD_GL_GROUP AND DM.COD_BUSINESS_LINE = ACT.COD_BUSINESS_LINE 
AND DM.COD_PL_ACC = ACT.COD_PL_ACC AND DM.COD_BLCE_PROD = ACT.COD_BLCE_PROD 
AND DM.COD_ENTITY = ACT.COD_ENTITY AND DM.COD_SEGMENT = ACT.COD_SEGMENT AND DM.COD_ACCO_CENT = ACT.COD_ACCO_CENT
AND DM.COD_EXPENSE = ACT.COD_EXPENSE AND DM.EXP_FAM = ACT.EXP_FAM AND DM.EXP_TYPE = ACT.EXP_TYPE 
AND DM.COD_CURRENCY = ACT.COD_CURRENCY AND DM.ESCENARIO = ACT.ESCENARIO
AND DM.DATA_DATE = ACT.DATA_DATE AND DM.IND_MENSUALIZA = ACT.IND_MENSUALIZA
LEFT JOIN (
    SELECT ACU.DATA_DATE, ACU.COD_CONT, ACU.COD_GL_GROUP, ACU.COD_BUSINESS_LINE, ACU.COD_PL_ACC, ACU.COD_BLCE_PROD, ACU.COD_CURRENCY, ACU.IND_MENSUALIZA, 
    SUM(ACU.PL) AS PL, SUM(ACU.EOPBAL_CAP) AS EOPBAL_CAP, SUM(ACU.AVGBAL_CAP) AS AVGBAL_CAP, SUM(ACU.INI_AM) AS INI_AM,
    ACU.COD_ENTITY, ACU.COD_SEGMENT, ACU.COD_ACCO_CENT, ACU.COD_EXPENSE, ACU.EXP_TYPE, ACU.EXP_FAM, SUM(ACU.NUM_CTOS) AS NUM_CTOS, ACU.ESCENARIO 
    FROM MIS_APR_BUDGET ACU 
    GROUP BY ACU.DATA_DATE, ACU.COD_CONT, ACU.COD_GL_GROUP, ACU.COD_BUSINESS_LINE, ACU.COD_PL_ACC, ACU.COD_BLCE_PROD, ACU.COD_CURRENCY, ACU.IND_MENSUALIZA, 
    ACU.COD_ENTITY, ACU.COD_SEGMENT, ACU.COD_ACCO_CENT, ACU.COD_EXPENSE, ACU.EXP_TYPE, ACU.EXP_FAM, ACU.ESCENARIO) ACU
ON DM.COD_GL_GROUP = ACU.COD_GL_GROUP AND DM.COD_BUSINESS_LINE = ACU.COD_BUSINESS_LINE 
AND DM.COD_PL_ACC = ACU.COD_PL_ACC AND DM.COD_BLCE_PROD = ACU.COD_BLCE_PROD 
AND DM.COD_ENTITY = ACU.COD_ENTITY AND  DM.COD_SEGMENT = ACU.COD_SEGMENT AND DM.COD_ACCO_CENT = ACU.COD_ACCO_CENT
AND DM.COD_EXPENSE = ACT.COD_EXPENSE AND DM.EXP_FAM = ACT.EXP_FAM AND DM.EXP_TYPE = ACT.EXP_TYPE 
AND DM.COD_CURRENCY = ACU.COD_CURRENCY AND DM.ESCENARIO = ACU.ESCENARIO 
AND DM.IND_MENSUALIZA = ACU.IND_MENSUALIZA AND 
TO_TIMESTAMP(ACU.DATA_DATE, 'yyyyMMdd') BETWEEN TRUNC(TO_TIMESTAMP(DM.DATA_DATE, 'yyyyMMdd') ,'Y') AND TO_TIMESTAMP(DM.DATA_DATE,'yyyyMMdd')
LEFT JOIN (
    SELECT PRV.DATA_DATE, PRV.COD_CONT, PRV.COD_GL_GROUP, PRV.COD_BUSINESS_LINE, PRV.COD_PL_ACC, PRV.COD_BLCE_PROD, PRV.COD_CURRENCY, PRV.IND_MENSUALIZA, 
    SUM(PRV.PL) AS PL, SUM(PRV.EOPBAL_CAP) AS EOPBAL_CAP, SUM(PRV.AVGBAL_CAP) AS AVGBAL_CAP, SUM(PRV.INI_AM) AS INI_AM,
    PRV.COD_ENTITY, PRV.COD_SEGMENT, PRV.COD_ACCO_CENT, PRV.COD_EXPENSE, PRV.EXP_FAM, PRV.EXP_TYPE,  SUM(PRV.NUM_CTOS) AS NUM_CTOS, PRV.ESCENARIO 
    FROM MIS_APR_BUDGET PRV
    GROUP BY PRV.DATA_DATE, PRV.COD_CONT, PRV.COD_GL_GROUP, PRV.COD_BUSINESS_LINE, PRV.COD_PL_ACC, PRV.COD_BLCE_PROD, PRV.COD_CURRENCY, PRV.IND_MENSUALIZA, 
    PRV.COD_ENTITY, PRV.COD_SEGMENT, PRV.COD_ACCO_CENT, PRV.COD_EXPENSE, PRV.EXP_FAM,  PRV.EXP_TYPE,PRV.ESCENARIO) PRV
ON DM.COD_GL_GROUP = PRV.COD_GL_GROUP AND DM.COD_BUSINESS_LINE = PRV.COD_BUSINESS_LINE 
AND DM.COD_PL_ACC = PRV.COD_PL_ACC AND DM.COD_BLCE_PROD = PRV.COD_BLCE_PROD 
AND DM.COD_ENTITY = PRV.COD_ENTITY AND DM.COD_SEGMENT = PRV.COD_SEGMENT AND DM.COD_ACCO_CENT = PRV.COD_ACCO_CENT 
AND DM.COD_EXPENSE = ACT.COD_EXPENSE AND DM.EXP_FAM = ACT.EXP_FAM AND DM.EXP_TYPE = ACT.EXP_TYPE 
AND DM.COD_CURRENCY = PRV.COD_CURRENCY AND DM.ESCENARIO = PRV.ESCENARIO 
AND DM.IND_MENSUALIZA = PRV.IND_MENSUALIZA AND  
TO_TIMESTAMP(PRV.DATA_DATE, 'yyyyMMdd') = ADD_MONTHS(TO_TIMESTAMP(DM.DATA_DATE, 'yyyyMMdd'),-1) AND SUBSTR(DM.DATA_DATE, 5, 2) <> '01'
WHERE SUBSTR(DM.DATA_DATE, 1, 4) = SUBSTR('${var:periodo}', 1, 4) AND 
NOT EXISTS (SELECT 1
            FROM MIS_DM_BUDGET
            WHERE ESCENARIO = DM.ESCENARIO AND STRLEFT(DATA_DATE,4) = STRLEFT(DM.DATA_DATE,4))
GROUP BY DM.DATA_DATE, ACT.COD_CONT, DM.COD_GL_GROUP, DM.COD_BUSINESS_LINE, DM.COD_PL_ACC, DM.COD_BLCE_PROD, DM.COD_CURRENCY, DM.IND_MENSUALIZA,
ACT.EOPBAL_CAP, ACT.AVGBAL_CAP, ACT.INI_AM, DM.COD_ENTITY, DM.COD_SEGMENT, DM.COD_ACCO_CENT, DM.COD_EXPENSE, DM.EXP_TYPE,
DM.EXP_FAM, ACT.NUM_CTOS, DM.ESCENARIO
HAVING (SUM(IFNULL(ACT.EOPBAL_CAP, 0)) + SUM(IFNULL(ACT.AVGBAL_CAP, 0)) + SUM(IFNULL(ACT.PL, 0)) + SUM(IFNULL(ACT.INI_AM, 0))) != 0
ORDER BY DM.COD_PL_ACC ASC, DM.COD_BLCE_PROD ASC, DM.DATA_DATE ASC;