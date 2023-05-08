#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Pasivos'

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
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_fuentes_pasivos=*)
    ruta_fuentes_pasivos="${i#*=}"
    shift
    ;;
    incluir_fuentes_tardias=*)
    incluir_fuentes_tardias="${i#*=}"
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

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_pasivos}
export RUTA_DESTINO=${ruta_fuentes_pasivos}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/AHBKR001CD.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ahbkr001CD_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/AHBKR001CD.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/AHBKR001CM.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ahbkr001CM_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/AHBKR001CM.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CCBKR001.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ccbkr001_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CCBKR001.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/AHRETENCION.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ahretencion_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/AHRETENCION.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CCRETENCION.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ccretencion_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CCRETENCION.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MBALANCE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/mbalance_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MBALANCE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CUVENORE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/cuvenore_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CUVENORE.CSV

if [ "${incluir_fuentes_tardias}" = "SI" ]; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_100.CSV
hdfs dfs -cp $RUTA_ORIGEN/Estado_de_Portafolio_100_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/ESTADO_PORTAFOLIO_100.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_87.CSV
hdfs dfs -cp $RUTA_ORIGEN/Estado_de_Portafolio_87_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/ESTADO_PORTAFOLIO_87.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_88.CSV
hdfs dfs -cp $RUTA_ORIGEN/Estado_de_Portafolio_88_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/ESTADO_PORTAFOLIO_88.CSV

else 

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_100.CSV
hdfs dfs -touchz $RUTA_DESTINO/ESTADO_PORTAFOLIO_100.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_87.CSV
hdfs dfs -touchz $RUTA_DESTINO/ESTADO_PORTAFOLIO_87.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ESTADO_PORTAFOLIO_88.CSV
hdfs dfs -touchz $RUTA_DESTINO/ESTADO_PORTAFOLIO_88.CSV

fi