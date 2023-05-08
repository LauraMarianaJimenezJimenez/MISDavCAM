#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Activos'

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
    ruta_origen_activos=*)
    ruta_origen_activos="${i#*=}"
    shift
    ;;
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
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
NEXT=$(date -d "${PERIODO} +1 day" +%d)

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"
echo "Siguiente dia: $NEXT"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_activos}
export RUTA_DESTINO=${ruta_fuentes_activos}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/CAINFOPER.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Cainfoper_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CAINFOPER.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INFO_TRABAJO.CSV
hdfs dfs -cp  $RUTA_ORIGEN/InfoTrabajo_$YEAR$MONTH.csv $RUTA_DESTINO/INFO_TRABAJO.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/LISTA_MOV.CSV
hdfs dfs -cp  $RUTA_ORIGEN/ListaMov_$YEAR$MONTH.csv $RUTA_DESTINO/LISTA_MOV.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TARENTE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/TarEnte_$YEAR$MONTH.csv $RUTA_DESTINO/TARENTE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TCS.CSV
hdfs dfs -cp  $RUTA_ORIGEN/TCS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/TCS.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/LCS.CSV
hdfs dfs -cp  $RUTA_ORIGEN/LCS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/LCS.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_A2.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_A2.csv $RUTA_DESTINO/MV_A2.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_BANX.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_BANX.csv $RUTA_DESTINO/MV_BANX.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_MC.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_MC.csv $RUTA_DESTINO/MV_MC.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_ONE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_ONE.csv $RUTA_DESTINO/MV_ONE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_A1.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_A1.csv $RUTA_DESTINO/MV_A1.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MV_DEBT.CSV
hdfs dfs -cp  $RUTA_ORIGEN/MV_${YEAR}${MONTH}${DAY}_DEBT.csv $RUTA_DESTINO/MV_DEBT.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TARJETAS_ENTREGADAS.CSV
hdfs dfs -cp  $RUTA_ORIGEN/TarjetasEntregadas_${YEAR}${MONTH}${DAY}.csv $RUTA_DESTINO/TARJETAS_ENTREGADAS.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TARJETAS_SUBPRODUCTO.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Subproductos_${YEAR}${MONTH}${DAY}.csv $RUTA_DESTINO/TARJETAS_SUBPRODUCTO.CSV

#Inserción de tarjetas adicionales
if hdfs dfs -test -e $RUTA_ORIGEN/TarjetasAdicionales_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TarjetasAdicionales.csv
hdfs dfs -cp $RUTA_ORIGEN/TarjetasAdicionales_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/TarjetasAdicionales.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/TarjetasAdicionales.csv
hdfs dfs -touchz $RUTA_DESTINO/TarjetasAdicionales.csv

fi


if [ "${incluir_fuentes_tardias}" = "SI" ]; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INT_OPE.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Interes_Operacion_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/INT_OPE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INVERSIONES.CSV
hdfs dfs -cp  $RUTA_ORIGEN/Auxiliar_Inversiones_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/INVERSIONES.CSV

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INT_OPE.CSV
hdfs dfs -touchz $RUTA_DESTINO/INT_OPE.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/INVERSIONES.CSV
hdfs dfs -touchz $RUTA_DESTINO/INVERSIONES.CSV

fi