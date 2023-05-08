----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
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
    ruta_origen_seguros=*)
    ruta_origen_seguros="${i#*=}"
    shift
    ;;
    ruta_origen_seg_contabilidad=*)
    ruta_origen_seg_contabilidad="${i#*=}"
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
NEXT=$(date -d "${PERIODO} +1 day" +%d)

--- Impresión de verificación de Fecha ---
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"
echo "Siguiente dia: $NEXT"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_contabilidad}
export RUTA_ORIGEN_SEG=${ruta_origen_seguros}
export RUTA_DESTINO=${ruta_fuentes_contabilidad}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/GLP00301.csv
hadoop dfs -cp $RUTA_ORIGEN/GLP00301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLP00301.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/GLP01301.csv
hadoop dfs -cp $RUTA_ORIGEN/GLP01301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLP01301.csv

#hadoop dfs -rm -skipTrash $RUTA_DESTINO/GL_CODE_COMBINATIONS.csv
#hadoop dfs -cp $RUTA_ORIGEN_SEG/GL_CODE_COMBINATIONS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_CODE_COMBINATIONS.csv

#hadoop dfs -rm -skipTrash $RUTA_DESTINO/GL_JE_HEADERS.csv
#hadoop dfs -cp $RUTA_ORIGEN_SEG/GL_JE_HEADERS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_JE_HEADERS.csv

#hadoop dfs -rm -skipTrash $RUTA_DESTINO/GL_JE_LINES.csv
#hadoop dfs -cp $RUTA_ORIGEN_SEG/GL_JE_LINES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GL_JE_LINES.csv

if [ $NEXT -eq 01 ] ; then

hadoop dfs -rm -skipTrash $RUTA_DESTINO/FUENTE_SEGUROS.csv
hadoop dfs -cp $RUTA_ORIGEN_SEG/Fuente_Seguros_$YEAR$MONTH.csv $RUTA_DESTINO/FUENTE_SEGUROS.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/FUENTE_SEGUROS.csv
hdfs dfs -touchz $RUTA_DESTINO/FUENTE_SEGUROS.csv

fi