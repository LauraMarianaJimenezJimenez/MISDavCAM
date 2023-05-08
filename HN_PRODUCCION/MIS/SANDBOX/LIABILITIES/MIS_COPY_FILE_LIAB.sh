----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Activos'

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
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_fuentes_pasivos=*)
    ruta_fuentes_pasivos="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_pasivos}
export RUTA_DESTINO=${ruta_fuentes_pasivos}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/TAP00201.csv
hadoop dfs -cp $RUTA_ORIGEN/TAP00201_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/TAP00201.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/TMP003.csv
hadoop dfs -cp $RUTA_ORIGEN/TMP003_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/TMP003.csv

#hadoop dfs -rm -skipTrash $RUTA_DESTINO/CFP31001.csv
#hadoop dfs -cp $RUTA_ORIGEN/CFP31001_$YEAR$MONTH.csv $RUTA_DESTINO/CFP31001.csv

#hadoop dfs -rm -skipTrash $RUTA_DESTINO/CFP21001.csv
#hadoop dfs -cp $RUTA_ORIGEN/CFP21001_$YEAR$MONTH.csv $RUTA_DESTINO/CFP21001.csv