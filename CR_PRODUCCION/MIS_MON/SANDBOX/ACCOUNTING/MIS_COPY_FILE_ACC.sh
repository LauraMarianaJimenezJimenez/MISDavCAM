#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Contabilidad Mensual'

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
export RUTA_ORIGEN=${ruta_origen_contabilidad}
export RUTA_DESTINO=${ruta_fuentes_contabilidad}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/BALCO.CSV
hdfs dfs -cp $RUTA_ORIGEN/cb_balcobx_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/BALCO.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/BAL_SEGUROS.CSV
hdfs dfs -cp $RUTA_ORIGEN/Balance_Seguro_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/BAL_SEGUROS.CSV

#hdfs dfs -rm -skipTrash $RUTA_DESTINO/BASE_GASTO.CSV
#hdfs dfs -cp $RUTA_ORIGEN/Base_Gastos_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/BASE_GASTO.CSV