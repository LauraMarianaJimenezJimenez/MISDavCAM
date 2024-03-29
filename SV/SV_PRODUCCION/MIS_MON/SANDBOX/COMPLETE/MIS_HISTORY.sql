---------- MOTOR DE GESTION DE INFORMACION ----------


--- Uso de la base de datos correcta ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

--- Borrado de Tablas Mensuales ---

ALTER TABLE MIS_DWH_ACCOUNTING_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DWH_ASSETS_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DWH_LIABILITIES_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DWH_FEES_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DWH_OFF_BALANCE_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DWH_PROVISIONS_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DM_BALANCE_RESULT_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-60),'yyyyMMdd'));

ALTER TABLE MIS_DWH_RECONCILIATION_M
DROP IF EXISTS PARTITION (DATA_DATE = FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-12),'yyyyMMdd'));

ALTER TABLE MIS_DM_BUDGET
DROP IF EXISTS PARTITION (YEAR = STRLEFT(FROM_TIMESTAMP(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}','yyyyMMdd'),-25),'yyyyMMdd'),4));