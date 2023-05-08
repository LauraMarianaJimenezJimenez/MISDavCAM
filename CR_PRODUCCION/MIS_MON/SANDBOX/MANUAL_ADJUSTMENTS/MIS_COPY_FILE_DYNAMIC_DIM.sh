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

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_ajustes}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
#DIMENSIONES DINAMICAS

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Ajustes_Dimensiones_Dinamicas.csv
hdfs dfs -cp $RUTA_ORIGEN/Ajustes_Dimensiones_Dinamicas.csv $RUTA_DESTINO/Ajustes_Dimensiones_Dinamicas.csv