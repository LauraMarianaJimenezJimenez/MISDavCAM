#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

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
    ruta_origen_rentabilidad=*)
    ruta_origen_rentabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
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

#--- Definición de variables de Fecha ---
D=${PERIODO}
DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
NEXT=$(date -d "${PERIODO} +1 day" +%d)

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"
echo "Siguiente dia: $NEXT"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_rentabilidad}
export RUTA_ORIGEN_PARAMETRIA=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_contabilidad}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/GLMST.CSV
hdfs dfs -cp $RUTA_ORIGEN/GLMST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLMST.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/GLBLN.CSV
hdfs dfs -cp $RUTA_ORIGEN/GLBLN_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLBLN.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/WFSALDOS.CSV
hdfs dfs -cp $RUTA_ORIGEN/WFSALDOS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/WFSALDOS.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/SALDOSCON.CSV
hdfs dfs -cp $RUTA_ORIGEN/SALDOSCON_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/SALDOSCON.CSV

if [ $NEXT -eq 01 ] ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Carga_Manual_Valores.csv
hdfs dfs -cp $RUTA_ORIGEN_PARAMETRIA/Carga_Manual_Valores_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/Carga_Manual_Valores.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/Carga_Manual_Valores.csv
hdfs dfs -touchz $RUTA_DESTINO/Carga_Manual_Valores.csv

fi
