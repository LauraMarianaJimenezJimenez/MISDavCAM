----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

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
    ruta_origen_comisiones=*)
    ruta_origen_comisiones="${i#*=}"
    shift
    ;;
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_origen_transacciones=*)
    ruta_origen_transacciones="${i#*=}"
    shift
    ;;
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
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
MONTH_ANT=$(date -d "${PERIODO} -1 month" +%Y%m)
MONTH_ANT2=$(date -d "${PERIODO} -32 day" +%Y%m)

--- Impresión de verificación de Fecha ---
echo "MONTH_ANT: $MONTH_ANT"
echo "MONTH_ANT2: $MONTH_ANT2"
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN=${ruta_origen_activos}
export RUTA_ORIGEN_COM=${ruta_origen_comisiones}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_ORIGEN_TRANS=${ruta_origen_transacciones}
export RUTA_DESTINO=${ruta_fuentes_activos}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/CR_01.csv
hadoop dfs -cp $RUTA_ORIGEN/CR_01_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CR_01.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CR_91.csv
hadoop dfs -cp $RUTA_ORIGEN/CR_91_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CR_91.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/LNP00301.csv
hadoop dfs -cp $RUTA_ORIGEN/LNP00301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/LNP00301.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CUP00301.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/CUP00301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CUP00301.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CUP00901.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/CUP00901_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CUP00901.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/IN_10MST.csv
hadoop dfs -cp $RUTA_ORIGEN/IN_10MST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/IN_10MST.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/GLC002.csv
hadoop dfs -cp $RUTA_ORIGEN_COM/GLC002_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/GLC002.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/S95TRA.csv
hadoop dfs -cp $RUTA_ORIGEN/S95TRA_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/S95TRA.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/INE10MST.csv
hadoop dfs -cp $RUTA_ORIGEN/INE10MST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/INE10MST.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SGBTRA.csv
hadoop dfs -cp $RUTA_ORIGEN/SGBTRA_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/SGBTRA.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/TN1DET07.csv
hadoop dfs -cp $RUTA_ORIGEN/TN1DET07_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/TN1DET07.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PREMIACION.csv
hadoop dfs -cp $RUTA_ORIGEN/MIS_PREMIACION_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PREMIACION.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/CFP50301.csv
hadoop dfs -cp $RUTA_ORIGEN/CFP50301_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CFP50301.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SJ0TRA.csv
hadoop dfs -cp $RUTA_ORIGEN/SJ0TRA_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/SJ0TRA.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/INPLA031.csv
hadoop dfs -cp $RUTA_ORIGEN/INPLA031_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/INPLA031.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SGWTRA.csv
hadoop dfs -cp $RUTA_ORIGEN/SGWTRA_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/SGWTRA.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MAESTTARJ.csv
hadoop dfs -cp $RUTA_ORIGEN/MAESTTARJ_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MAESTTARJ.csv

if [ $MONTH -eq 03 && $DAY -ge 28 ] ; then

echo "entro en la 1"

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/MIS_SEGMENTO_BANCA_PERSONAS_$MONTH_ANT2.csv $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/SEGMENTO_BANCA_EMPRESAS_$MONTH_ANT2.csv $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv

else
if [ $DAY -eq 31 ] ; then

echo "entro en la 2"

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/MIS_SEGMENTO_BANCA_PERSONAS_$MONTH_ANT2.csv $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/SEGMENTO_BANCA_EMPRESAS_$MONTH_ANT2.csv $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv

else

echo "entro en la 3"

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/MIS_SEGMENTO_BANCA_PERSONAS_$MONTH_ANT.csv $RUTA_DESTINO/MIS_SEGMENTO_BANCA_PERSONAS.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv
hadoop dfs -cp $RUTA_ORIGEN_CLIENTES/SEGMENTO_BANCA_EMPRESAS_$MONTH_ANT.csv $RUTA_DESTINO/SEGMENTO_BANCA_EMPRESAS.csv

fi
fi