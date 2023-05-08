#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de ROF'

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
    ruta_origen_rof=*)
    ruta_origen_rof="${i#*=}"
    shift
    ;;
    ruta_fuentes_rof=*)
    ruta_fuentes_rof="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_rof}
export RUTA_DESTINO=${ruta_fuentes_rof}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---


if [ "${incluir_fuentes_tardias}" = "SI" ]; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INTRADAY.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Intraday_${YEAR}${MONTH}${DAY}.csv $RUTA_DESTINO/INTRADAY.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/BROKERAGE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Brokerage_${YEAR}${MONTH}${DAY}.csv $RUTA_DESTINO/BROKERAGE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MAN_TRAN.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ManualTransactions_${YEAR}${MONTH}${DAY}.csv $RUTA_DESTINO/MAN_TRAN.CSV

else 

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INTRADAY.CSV
hdfs dfs -touchz $RUTA_DESTINO/INTRADAY.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/BROKERAGE.CSV
hdfs dfs -touchz $RUTA_DESTINO/BROKERAGE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MAN_TRAN.CSV
hdfs dfs -touchz $RUTA_DESTINO/MAN_TRAN.CSV

fi