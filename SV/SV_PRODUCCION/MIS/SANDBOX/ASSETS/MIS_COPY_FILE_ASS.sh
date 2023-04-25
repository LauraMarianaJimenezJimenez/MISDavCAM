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
    ruta_origen_rentabilidad=*)
    ruta_origen_rentabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
    shift
    ;;
    ruta_fuentes_clientes=*)
    ruta_fuentes_clientes="${i#*=}"
    shift
    ;;
    ruta_fuentes_pasivos=*)
    ruta_fuentes_pasivos="${i#*=}"
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
export RUTA_ORIGEN=${ruta_origen_activos}
export RUTA_ORIGEN_RENTABILIDAD=${ruta_origen_rentabilidad}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_DESTINO=${ruta_fuentes_activos}
export RUTA_DESTINO_CLIENTES=${ruta_fuentes_clientes}
export RUTA_ORIGEN_PASIVOS=${ruta_origen_pasivos}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/DEALS.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/DEALS_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/DEALS.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/VPSCUENT.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/VPSCUENT_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/VPSCUENT.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/VPSATSMP.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/VPSATSMP_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/VPSATSMP.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO_CLIENTES/WFCUMST.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/WFCUMST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO_CLIENTES/WFCUMST.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/VPSPLAN.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/VPSPLAN_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/VPSPLAN.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/GLSOB.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/GLSOB_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLSOB.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/SOBDT.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/SOBDT_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/SOBDT.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CSEXTMSTF.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CSEXTMSTF_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CSEXTMSTF.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CSPLNINTF.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CSPLNINTF_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CSPLNINTF.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/DLAHI.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/DLAHI_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/DLAHI.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CREMST.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CREMST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CREMST.CSV

hdfs dfs -rm -skipTrash $RUTA_DESTINO/ACMST.CSV
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/ACMST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/ACMST.CSV
