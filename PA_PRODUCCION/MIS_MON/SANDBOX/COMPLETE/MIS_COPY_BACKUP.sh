----- PROCESO DE COPIA DE ARCHIVOS DE PARAMETRIA CONTABLE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Contabilidad'

for i in "$@" 
do
case $i in
    usuario_oozie=*)
    USUARIO_OOZIE="${i#*=}"
    shift
    ;;
    usuario_dominio=*)
    USUARIO_DOMINIO="${i#*=}"
    shift
    ;;
    periodo=*)
    PERIODO="${i#*=}"
    shift
    ;;
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_backup=*)
    ruta_backup="${i#*=}"
    shift
    ;;
    *)
    shift
    ;;
esac
done

--- Definición de variables de Fecha ---
D=${PERIODO}
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
dateTemp=$(date -d "${YEAR}${MONTH}01 + 1 month")
echo "tmp $dateTemp"
D=$(date -d "$dateTemp - 1 day" "+%Y%m%d")

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
FECHA=$(date '+%Y%m%d')

--- Impresión de verificación de Fecha ---
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_backup}

echo "RUTA ORIGEN $RUTA_ORIGEN_PAR"
echo "RUTA_DESTINO $RUTA_DESTINO"

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Impresión de verificación de la ruta de PYTHON EGG CACHE ---
echo 'ruta python' $PYTHON_EGG_CACHE

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_CAF_ACC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_ACC.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_CAF_ACC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_PL_ACC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PL_ACC.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_PL_ACC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BLCE_PROD_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BLCE_PROD.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BLCE_PROD_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BL_ACC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_ACC.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BL_ACC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_BL_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_BL_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_CAF_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CAF.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_CAF_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_PL_ACC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL_ACC.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_PL_ACC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_BLCE_PROD_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BLCE_PROD.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_BLCE_PROD_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BLCE_PROD_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BLCE_PROD_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PL_ACC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PL_ACC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BL_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BL.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BL_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_UN_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_UN.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_UN_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_REG_DIMENSIONS_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_REG_DIMENSIONS_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_MUL_LAT_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_MUL_LAT_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_CONV_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_CONV_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_CAF_OPER_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_CAF_OPER_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BLCE_PROD_OPER_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BLCE_PROD_OPER.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BLCE_PROD_OPER_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BL_OPER_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_BL_OPER_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_SECTOR_ECO_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_SECTOR_ECO_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_EXP_TYP_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_EXP_TYP_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_ECO_GRO_REG_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_CAT_ECO_GRO_REG_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_ECO_GRO_REG_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_ECO_GRO_REG_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PROD_BL_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PROD_BL_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BLCE_PROD_LC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_BLCE_PROD_LC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PL_ACC_LC_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/$FECHA$"_MIS_HIERARCHY_PL_ACC_LC_"$YEAR$MONTH$DAY.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_PROD_SPE_"$YEAR$MONTH$DAY.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROD_SPE.csv $RUTA_DESTINO/$FECHA$"_MIS_PAR_REL_PROD_SPE_"$YEAR$MONTH$DAY.csv