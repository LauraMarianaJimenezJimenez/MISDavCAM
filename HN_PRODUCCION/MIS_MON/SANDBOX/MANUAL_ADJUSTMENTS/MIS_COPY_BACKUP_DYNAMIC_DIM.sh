----- PROCESO DE COPIA DE ARCHIVOS DE DIMENSIONES DINAMICAS A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de dimensiones dinamicas'

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
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_backup=*)
    ruta_backup="${i#*=}"
    shift
    ;;
    *)
    shift
    ;;
esac
done

--- Definición de variables de Fecha ---
FECHA=$(date '+%Y%m%d')

--- Impresión de verificación de Fecha ---
echo "Fecha: $FECHA"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_backup}

echo "RUTA ORIGEN $RUTA_ORIGEN_PAR"
echo "RUTA_DESTINO $RUTA_DESTINO"

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Impresión de verificación de la ruta de PYTHON EGG CACHE ---
echo 'ruta python' $PYTHON_EGG_CACHE

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_Ajustes_Dimensiones_Dinamicas.csv"
hadoop dfs -cp $RUTA_ORIGEN_PAR/Ajustes_Dimensiones_Dinamicas.csv $RUTA_DESTINO/$FECHA$"_Ajustes_Dimensiones_Dinamicas.csv"