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
    ruta_origen_comisiones=*)
    ruta_origen_comisiones="${i#*=}"
    shift
    ;;
    ruta_fuentes_comisiones=*)
    ruta_fuentes_comisiones="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_comisiones}
export RUTA_DESTINO=${ruta_fuentes_comisiones}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/Comisiones_Trade.csv
hadoop dfs -cp $RUTA_ORIGEN/Comisiones_Trade_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Comisiones_Trade.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/Subastas_Consolidadas.csv
hadoop dfs -cp $RUTA_ORIGEN/Subastas_Consolidadas_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Subastas_Consolidadas.csv