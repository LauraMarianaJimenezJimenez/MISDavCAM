----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos fuente de Fuera de balance'

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
    ruta_origen_activos=*)
    ruta_origen_activos="${i#*=}"
    shift
    ;;
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_origen_referenciales=*)
    ruta_origen_referenciales="${i#*=}"
    shift
    ;;
    ruta_fuentes_pasivos=*)
    ruta_fuentes_pasivos="${i#*=}"
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
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
dateTemp=$(date -d "${YEAR}${MONTH}01 + 1 month")
echo "tmp $dateTemp"
D=$(date -d "$dateTemp - 1 day" "+%Y%m%d")

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_ACT=${ruta_origen_activos}
export RUTA_ORIGEN_PAS=${ruta_origen_pasivos}
export RUTA_ORIGEN_REF=${ruta_origen_referenciales}
export RUTA_DESTINO=${ruta_fuentes_activos}
export RUTA_DESTINO_PAS=${ruta_fuentes_pasivos}


--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO_PAS/CFP31001.csv
hadoop dfs -cp $RUTA_ORIGEN_PAS/CFP31001_$YEAR$MONTH$DAY.csv $RUTA_DESTINO_PAS/CFP31001.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO_PAS/CFP21001.csv
hadoop dfs -cp $RUTA_ORIGEN_PAS/CFP21001_$YEAR$MONTH$DAY.csv $RUTA_DESTINO_PAS/CFP21001.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CFP50301.csv
hadoop dfs -cp $RUTA_ORIGEN_ACT/CFP50301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CFP50301.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CFP10301.csv
hadoop dfs -cp $RUTA_ORIGEN_REF/CFP10301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CFP10301.csv