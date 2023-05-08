----- PROCESO DE COPIA DE ARCHIVOS PARAMETRICOS A CARPETAS PROPIAS DEL MIS -----

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
    periodo=*)
    PERIODO="${i#*=}"
    shift
    ;;
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_destino=*)
    ruta_destino="${i#*=}"
    shift
    ;;
    *)
    shift
    ;;
esac
done


--- Definición de variables de Fecha ---
D=${PERIODO}
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
dateTemp=$(date -d "${YEAR}${MONTH}01 + 1 month")
echo "tmp $dateTemp"
D=$(date -d "$dateTemp - 1 day" "+%Y%m%d")
FECHA=$(date '+%Y%m%d')


DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_destino}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}


hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
hdfs dfs -touchz $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

echo "Faltan:" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

--- Validacion fuentes de Parametría ---

echo "Ruta Parametría"
echo "$RUTA_ORIGEN_PAR"
echo "Ruta Destino"
echo "$RUTA_DESTINO"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Parametría" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_ENG_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Presupuesto" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


hadoop dfs -test -e $RUTA_ORIGEN_PAR/PRESUPUESTO_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "PRESUPUESTO_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Ajustes Manuales" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


hadoop dfs -test -e $RUTA_ORIGEN_PAR/Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi


hadoop dfs -test -e $RUTA_ORIGEN_PAR/Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi


echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Provisiones" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


hadoop dfs -test -e $RUTA_ORIGEN_PAR/TA_CONT_PAN_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "TA_CONT_PAN_$YEAR$MONTH$DAY.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi


hadoop dfs -test -e $RUTA_ORIGEN_PAR/PROV_INV_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "PROV_INV_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi