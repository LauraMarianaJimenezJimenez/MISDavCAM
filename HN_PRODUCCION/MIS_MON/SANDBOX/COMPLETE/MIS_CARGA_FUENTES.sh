----- PROCESO DE COPIA DE ARCHIVOS PARAMETRICOS A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos parametricos de Activos'

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
    ruta_origen_activos=*)
    ruta_origen_activos="${i#*=}"
    shift
    ;;
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_origen_referenciales=*)
    ruta_origen_referenciales="${i#*=}"
    shift
    ;;
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_destino=*)
    ruta_destino="${i#*=}"
    shift
    ;;
    ruta_origen_comisiones=*)
    ruta_origen_comisiones="${i#*=}"
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

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

FECHA=$(date '+%Y%m%d')


--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_ORIGEN_COM=${ruta_origen_comisiones}
export RUTA_ORIGEN_ACT=${ruta_origen_activos}
export RUTA_ORIGEN_PAS=${ruta_origen_pasivos}
export RUTA_ORIGEN_REF=${ruta_origen_referenciales}
export RUTA_DESTINO=${ruta_destino}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
hdfs dfs -touchz $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

echo "Faltan:" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt


--- Validacion fuentes de parametria ---

echo "Ruta parametria"
echo "$RUTA_ORIGEN_PAR"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Parametria" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

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
echo "MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TAX_ENG_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TAX_ENG_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
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

hadoop dfs -test -e $RUTA_ORIGEN_COM/TBL_DISTRI_CTAS_NIFF_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TBL_DISTRI_CTAS_NIFF_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_COM/TBL_REPORTE_FINAL_HIST_BC_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TBL_REPORTE_FINAL_HIST_BC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
echo "Catalogos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAS/CFP31001_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CFP31001_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/CFP21001_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CFP21001_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/CFP21001_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CFP21001_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/CFP50301_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CFP50301_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_REF/CFP10301_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CFP10301_$YEAR$MONTH$DAY.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_MES_"$YEAR$MONTH$DAY.txt
else
echo "El archivo existe"
fi