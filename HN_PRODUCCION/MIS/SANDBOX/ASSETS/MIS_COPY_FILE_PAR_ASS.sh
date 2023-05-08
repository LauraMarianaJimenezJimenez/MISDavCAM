----- PROCESO DE COPIA DE ARCHIVOS PARAMETRICOS A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos parametricos de Activos'

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
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
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
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_DESTINO=${ruta_fuentes_activos}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_CURVES.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv

else

echo "El archivo de curvas para el dia de ejecucion no se encontro, se replica el ultimo cargado"
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES.csv
hdfs dfs -cp $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv $RUTA_DESTINO/MIS_PAR_CURVES.csv

fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

fi

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_EXCH_RATE.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_EXCH_RATE.csv $RUTA_DESTINO/MIS_PAR_EXCH_RATE.csv

/*
#hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
#hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

fi

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PROG_CARD.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROG_CARD.csv $RUTA_DESTINO/MIS_PAR_REL_PROG_CARD.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SEGMENT.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SEGMENT.csv $RUTA_DESTINO/MIS_PAR_CAT_SEGMENT.csv