#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Provisiones'
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
    ruta_origen_provisiones=*)
    ruta_origen_provisiones="${i#*=}"
    shift
    ;;
    ruta_fuentes_provisiones=*)
    ruta_fuentes_provisiones="${i#*=}"
    shift
    ;;
    *)
    shift
    ;;
esac
done

#--- Definición de variables de Fecha ---
D=${PERIODO}
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
dateTemp=$(date -d "${YEAR}${MONTH}01 + 1 month")
echo "tmp $dateTemp"
D=$(date -d "$dateTemp - 1 day" "+%Y%m%d")

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_provisiones}
export RUTA_DESTINO=${ruta_fuentes_provisiones}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/BASE_RESERVAS.CSV
hdfs dfs -cp $RUTA_ORIGEN/Base_Reservas_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/BASE_RESERVAS.CSV


#--- Copia de los archivos del mes anterior, removiendo primero el archivo si existe ---
PREV=$(date -d "$(date -d "$PERIODO" '+%Y%m01') -1 day" '+%Y%m%d') #Ultimo dia del mes pasado
PDAY=$(date -d "$PREV" '+%d')
PMONTH=$(date -d "$PREV" '+%m')
PYEAR=$(date -d "$PREV" '+%Y')

hdfs dfs -rm -skipTrash $RUTA_DESTINO/BASE_RESERVAS_MESPREV.CSV
hdfs dfs -cp $RUTA_ORIGEN/Base_Reservas_$PYEAR$PMONTH$PDAY.csv $RUTA_DESTINO/BASE_RESERVAS_MESPREV.CSV