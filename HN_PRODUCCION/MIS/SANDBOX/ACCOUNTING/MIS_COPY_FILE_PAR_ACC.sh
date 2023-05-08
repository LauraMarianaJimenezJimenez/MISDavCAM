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
    ruta_fuentes_contabilidad=*)
    ruta_fuentes_contabilidad="${i#*=}"
    shift
    ;;
    *)
    shift
    ;;
esac
done

--- Definición de variables de Fecha ---
D=${PERIODO}
DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

--- Impresión de verificación de Fecha ---
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_contabilidad}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Impresión de verificación de la ruta de PYTHON EGG CACHE ---
echo 'ruta python' $PYTHON_EGG_CACHE

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BL.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv $RUTA_DESTINO/MIS_PAR_CAT_BL.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CAF.csv $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PL.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL.csv $RUTA_DESTINO/MIS_PAR_CAT_PL.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BP.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BP.csv $RUTA_DESTINO/MIS_PAR_CAT_BP.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

fi

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

fi

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BL.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_BL.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_UN.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_UN.csv $RUTA_DESTINO/MIS_HIERARCHY_UN.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv

/*
#hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
#hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv

fi

/*
#hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
#hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv

fi

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ACCO_CENT.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ACCO_CENT.csv $RUTA_DESTINO/MIS_PAR_CAT_ACCO_CENT.csv