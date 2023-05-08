----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
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
    ruta_origen_contabilidad=*)
    ruta_origen_contabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_prueba=*)
    ruta_origen_prueba="${i#*=}"
    shift
    ;;
    ruta_origen_comisiones=*)
    ruta_origen_comisiones="${i#*=}"
    shift
    ;;
    ruta_origen_activos=*)
    ruta_origen_activos="${i#*=}"
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
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_prueba}
export RUTA_ORIGEN_TRANS=${ruta_origen_prueba}
export RUTA_ORIGEN_EXC=${ruta_origen_prueba}

#export RUTA_ORIGEN=${ruta_origen_contabilidad}
#export RUTA_ORIGEN_TRANS=${ruta_origen_comisiones}
#export RUTA_ORIGEN_EXC=${ruta_origen_activos}
export RUTA_DESTINO=${ruta_fuentes_contabilidad}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/GLMST.CSV
hadoop dfs -cp $RUTA_ORIGEN/GLMST_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/GLMST.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/GLBLN.CSV
hadoop dfs -cp $RUTA_ORIGEN/GLBLN_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/GLBLN.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/TRANS.CSV
hadoop dfs -cp $RUTA_ORIGEN_TRANS/TRANS_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/TRANS.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/RTRNS.CSV
hadoop dfs -cp $RUTA_ORIGEN_EXC/RTRNS_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/RTRNS.CSV

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CCDSC.CSV
hadoop dfs -cp $RUTA_ORIGEN/CCDSC_$YEAR$MONTH$DAY.CSV $RUTA_DESTINO/CCDSC.CSV