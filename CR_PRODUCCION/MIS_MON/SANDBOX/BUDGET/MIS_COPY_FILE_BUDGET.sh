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
    ruta_origen_presupuesto=*)
    ruta_origen_presupuesto="${i#*=}"
    shift
    ;;
    ruta_fuentes_presupuesto=*)
    ruta_fuentes_presupuesto="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_presupuesto}
export RUTA_DESTINO=${ruta_fuentes_presupuesto}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
#Plantilla Presupuesto

if hdfs dfs -test -e $RUTA_ORIGEN/PRESUPUESTO_$YEAR$MONTH.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Presupuesto.csv
hdfs dfs -cp $RUTA_ORIGEN/PRESUPUESTO_$YEAR$MONTH.csv $RUTA_DESTINO/Presupuesto.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Presupuesto.csv
hdfs dfs -touchz $RUTA_DESTINO/Presupuesto.csv

fi
