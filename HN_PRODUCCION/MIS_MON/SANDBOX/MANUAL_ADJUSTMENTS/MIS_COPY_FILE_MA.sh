----- PROCESO DE COPIA DE ARCHIVOS FUENTE DE AJUSTES MANUALES-----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos fuente'

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
    ruta_fuentes_ajustes=*)
    ruta_fuentes_ajustes="${i#*=}"
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

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_parametria}
#export RUTA_ORIGEN_ACT=${ruta_origen_activos}
export RUTA_DESTINO=${ruta_fuentes_ajustes}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
#CONTABLES
hadoop dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Contables.csv
hdfs dfs -cp $RUTA_ORIGEN/Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Ajustes_Manuales_Contables.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Contables.csv
hdfs dfs -touchz $RUTA_DESTINO/Ajustes_Manuales_Contables.csv

fi

#OPERACIONALES
hadoop dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Operacionales.csv
hdfs dfs -cp $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Ajustes_Manuales_Operacionales.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Operacionales.csv
hdfs dfs -touchz $RUTA_DESTINO/Ajustes_Manuales_Operacionales.csv

fi

#TARJETAS ADICIONALES
hadoop dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Operacionales_TC_Adicionales.csv
hdfs dfs -cp $RUTA_ORIGEN/Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Ajustes_Manuales_Operacionales_TC_Adicionales.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_Operacionales_TC_Adicionales.csv
hdfs dfs -touchz $RUTA_DESTINO/Ajustes_Manuales_Operacionales_TC_Adicionales.csv

fi

#TTI
hadoop dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN/Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_TTI.csv
hdfs dfs -cp $RUTA_ORIGEN/Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Ajustes_Manuales_TTI.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Manuales_TTI.csv
hdfs dfs -touchz $RUTA_DESTINO/Ajustes_Manuales_TTI.csv

fi