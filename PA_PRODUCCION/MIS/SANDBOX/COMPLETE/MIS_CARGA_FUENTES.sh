----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos fuente de Activos'

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
    ruta_origen_prueba=*)
    ruta_origen_prueba="${i#*=}"
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
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_origen_contabilidad=*)
    ruta_origen_contabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_comisiones=*)
    ruta_origen_comisiones="${i#*=}"
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
export RUTA_ORIGEN_CONT=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_CONT"
export RUTA_ORIGEN_TRANS=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_TRANS"
export RUTA_ORIGEN_EXC=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_EXC"
export RUTA_ORIGEN_CLIENTES=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_CLIENTES"
export RUTA_ORIGEN_ACT=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_ACT"
export RUTA_ORIGEN_COM=${ruta_origen_prueba}
echo "RUTA_ORIGEN_COM"
export RUTA_ORIGEN_CLI=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_CLIENTES"
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
echo "$RUTA_ORIGEN_PAR"
export RUTA_ORIGEN_PAS=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_PAS"
export RUTA_ORIGEN_CON=${ruta_origen_prueba}
echo "$RUTA_ORIGEN_CON"
export RUTA_DESTINO=${ruta_destino}
echo "$RUTA_DESTINO"

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
hdfs dfs -touchz $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Faltan:" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

--- Validacion fuentes de contabilidad ---

echo "Ruta contabilidad"
echo "$RUTA_ORIGEN_CONT"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Contabilidad" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_CONT/GLMST_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "GLMST_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/GLBLN_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "GLBLN_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_TRANS/TRANS_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "TRANS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_EXC/RTRNS_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "RTRNS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/CCDSC_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "CCDSC_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/PROVEEDORES_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "PROVEEDORES_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/CONTABLE_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CONTABLE_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de parametria ---

echo "Ruta parametria"
echo "$RUTA_ORIGEN_PAR"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Parametria" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAF_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BLCE_PROD.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BLCE_PROD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CAF.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CAF.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BLCE_PROD.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BLCE_PROD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_UN.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_UN.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_REG_DIMENSIONS.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_MUL_LAT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CONV.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_ENG_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CURVES_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_SPE_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROD_SPE.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PROD_SPE.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAF_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BLCE_PROD_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BLCE_PROD_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BL_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_SECTOR.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_SECTOR.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_SEGMENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_SEGMENT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_SUBSEGMENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_SUBSEGMENT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_ENG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_ENG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_ENG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_ENG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_DRI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_DRI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_DRI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_OFFI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_OFFI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_EXP_TYP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CASTIGOS.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CASTIGOS.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CPS3S.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CPS3S.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CIERRES_TAR.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CIERRES_TAR.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAR_COVID.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAR_COVID.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_SECTOR_ECO.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_DATE_TAR.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_DATE_TAR.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PROD_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROG_CARD.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PROG_CARD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXC_DLAHI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_EXC_DLAHI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_DRI_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_ENG_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de clientes---

echo "Ruta clientes"
echo "$RUTA_ORIGEN_CLI"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Clientes" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_CLI/CNOFC_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "CNOFC_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CLI/APCLS_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "APCLS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de activos ---

echo "Ruta activos"
echo "$RUTA_ORIGEN_ACT"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Activos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_ACT/DEALS_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "DEALS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/AMOFE_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "AMOFE_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/CUMST_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "CUMST_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/S3STRA_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "S3STRA_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/SGWTRA_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "SGWTRA_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/DLAHI_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "DLAHI_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/S3NTRA_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "S3NTRA_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de Comisiones ---

echo "Ruta comisiones"
echo "$RUTA_ORIGEN_COM"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Comisiones" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_COM/MOV_$YEAR$MONTH$DAY.CSV 
if [ $? -ne 0 ] ; then
echo "MOV_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_COM/DCMST_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "DCMST_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de Pasivos ---

echo "Ruta pasivos"
echo "$RUTA_ORIGEN_PAS"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Pasivos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ACMST_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "ACMST_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/CNTRLRTE_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "CNTRLRTE_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de contingentes ---

echo "Ruta contingentes"
echo "$RUTA_ORIGEN_CON"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Contingentes" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_CON/LCMST_$YEAR$MONTH$DAY.CSV
if [ $? -ne 0 ] ; then
echo "LCMST_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi