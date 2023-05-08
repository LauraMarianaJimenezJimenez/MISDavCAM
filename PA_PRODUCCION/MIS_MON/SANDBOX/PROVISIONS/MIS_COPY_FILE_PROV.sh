----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos fuente de Provisiones'

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
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_fuentes_provisiones=*)
    ruta_fuentes_provisiones="${i#*=}"
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
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_activos}
export RUTA_ORIGEN_INV=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_provisiones}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_ORIGEN_INV/TA_CONT_PAN.CSV
hadoop dfs -cp $RUTA_ORIGEN_INV/TA_CONT_PAN_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/TA_CONT_PAN.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/PROV_INV.CSV
hadoop dfs -cp $RUTA_ORIGEN_INV/PROV_INV_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/PROV_INV.CSV