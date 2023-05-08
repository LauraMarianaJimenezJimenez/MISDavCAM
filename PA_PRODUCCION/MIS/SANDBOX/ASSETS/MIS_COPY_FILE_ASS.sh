----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos fuente de Activos'

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
    ruta_origen_prueba=*)
    ruta_origen_prueba="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_prueba}
export RUTA_ORIGEN_PAS=${ruta_origen_prueba}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_prueba}

#export RUTA_ORIGEN=${ruta_origen_activos}
#export RUTA_ORIGEN_PAS=${ruta_origen_pasivos}
#export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_DESTINO=${ruta_fuentes_activos}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/DEALS.CSV
hadoop dfs -cp $RUTA_ORIGEN/DEALS_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/DEALS.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/AMOFE.CSV
hadoop dfs -cp $RUTA_ORIGEN_PAS/AMOFE_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/AMOFE.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CUMST.CSV
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/CUMST_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/CUMST.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/S3STRA.CSV
hadoop dfs -cp $RUTA_ORIGEN/S3STRA_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/S3STRA.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SGWTRA.CSV
hadoop dfs -cp $RUTA_ORIGEN/SGWTRA_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/SGWTRA.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/DLAHI.CSV
hadoop dfs -cp $RUTA_ORIGEN/DLAHI_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/DLAHI.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/S3NTRA.CSV
hadoop dfs -cp $RUTA_ORIGEN/S3NTRA_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/S3NTRA.CSV