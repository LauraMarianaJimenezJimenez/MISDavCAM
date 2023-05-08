#----- PROCESO DE COPIA DE ARCHIVOS DE PARAMETRIA CONTABLE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de parametria contables'

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
    ruta_origen_presupuesto=*)
    ruta_origen_presupuesto="${i#*=}"
    shift
    ;;
    ruta_origen_ajustes=*)
    ruta_origen_ajustes="${i#*=}"
    shift
    ;;
    ruta_origen_curvas=*)
    ruta_origen_curvas="${i#*=}"
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

FECHA=$(date '+%Y%m%d')


#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_destino}
export RUTA_ORIGEN_PRESUPUESTO=${ruta_origen_presupuesto}
export RUTA_ORIGEN_CUR=${ruta_origen_curvas}
export RUTA_ORIGEN_AJUS=´${ruta_origen_ajustes}

echo "RUTA_DESTINO: $RUTA_DESTINO"

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---


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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_ENG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_BALAN_SEG_ENG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_DRI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_BALAN_SEG_DRI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TAX_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TAX_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

--- Validacion Persupuesto ---

echo "Ruta Presupuesto"
echo "$RUTA_ORIGEN_PRESUPUESTO"


echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Presupuesto" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


hadoop dfs -test -e $RUTA_ORIGEN_PRESUPUESTO/PRESUPUESTO_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "PRESUPUESTO_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi


--- Validacion Persupuesto ---

echo "Ruta Ajustes"
echo "$RUTA_ORIGEN_AJUS"


echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Ajustes" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


hadoop dfs -test -e $RUTA_ORIGEN_AJUS/Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Contables_$YEAR$MONTH$DAY.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_AJUS/Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Operacionales_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_AJUS/Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_Operacionales_TC_Adicionales_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_AJUS/Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Ajustes_Manuales_TTI_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi


