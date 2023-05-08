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
    ruta_origen_seguros=*)
    ruta_origen_seguros="${i#*=}"
    shift
    ;;
    ruta_fuentes_seguros=*)
    ruta_fuentes_seguros="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_seguros}
export RUTA_DESTINO=${ruta_fuentes_seguros}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm $RUTA_DESTINO/GL_CODE_COMBINATIONS.csv
hadoop dfs -cp $RUTA_ORIGEN/GL_CODE_COMBINATIONS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_CODE_COMBINATIONS.csv

hadoop dfs -rm $RUTA_DESTINO/GL_JE_HEADERS.csv
hadoop dfs -cp $RUTA_ORIGEN/GL_JE_HEADERS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_JE_HEADERS.csv

hadoop dfs -rm $RUTA_DESTINO/GL_JE_LINES.csv
hadoop dfs -cp $RUTA_ORIGEN/GL_JE_LINES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_JE_LINES.csv