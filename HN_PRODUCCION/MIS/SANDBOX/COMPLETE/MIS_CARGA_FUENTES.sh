----- PROCESO DE VERIFICACION DE FUENTES FALTANTES -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio de verificación de fuentes faltantes malla diaria'

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
    ruta_origen_contabilidad=*)
    ruta_origen_contabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_seguros=*)
    ruta_origen_seguros="${i#*=}"
    shift
    ;;
    ruta_origen_seg_contabilidad=*)
    ruta_origen_seg_contabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_origen_pasivos=*)
    ruta_origen_pasivos="${i#*=}"
    shift
    ;;
    ruta_origen_contingencias=*)
    ruta_origen_contingencias="${i#*=}"
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
MONTH2=$(date -d "${PERIODO} +1 day" +%d)
MONTH3=$(date -d "${PERIODO} -1 month" +%Y%m)
MONTH4=$(date -d "${PERIODO} +1 day" +%Y%m%d)
FECHA=$(date '+%Y%m%d')

--- Impresión de verificación de Fecha ---
echo "Fecha: $1"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"
echo "D: $D"
echo "Mes ant: $MONTH2"
echo "Mes ant1: $MONTH3"
echo "Mes ant2: $MONTH4"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_CONT=${ruta_origen_contabilidad}
export RUTA_ORIGEN_SEG=${ruta_origen_seguros}
export RUTA_ORIGEN_ACT=${ruta_origen_activos}
export RUTA_ORIGEN_COM=${ruta_origen_comisiones}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_ORIGEN_TRANS=${ruta_origen_transacciones}
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_ORIGEN_PAS=${ruta_origen_pasivos}
export RUTA_ORIGEN_CON=${ruta_origen_contingencias}
export RUTA_DESTINO=${ruta_destino}

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

hadoop dfs -test -e $RUTA_ORIGEN_CONT/GLP00301_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "GLP00301_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/GLP01301_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "GLP01301_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de seguros ---

echo "Ruta seguros"
echo "$RUTA_ORIGEN_SEG"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Seguros" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_SEG/FUENTE_SEGUROS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "FUENTE_SEGUROS_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion parametrias ---

echo "Ruta parametrias"
echo "$RUTA_ORIGEN_PAR"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Parametrias" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BP_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BP.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ECO_GRO.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CONV.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ACCO_CENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ACCO_CENT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de activos ---

echo "Ruta activos"
echo "$RUTA_ORIGEN_ACT"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Activos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_ACT/CR_01_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_CR01_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/CR_91_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CR_91_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/LNP00301_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "LNP00301_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CLIENTES/CUP00301_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CUP00301_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CLIENTES/CUP00901_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CUP00901_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/IN_10MST_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "IN_10MST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/S95TRA_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "S95TRA_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/INE10MST_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "INE10MST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/SGBTRA_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "SGBTRA_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/TN1DET07_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "TN1DET07_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MIS_PREMIACION_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PREMIACION_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes de comisiones ---

echo "Ruta comisiones"
echo "$RUTA_ORIGEN_COM"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Comisiones" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt


hadoop dfs -test -e $RUTA_ORIGEN_COM/GLC002_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "GLC002_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_COM/CDIF03_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CDIF03_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_COM/CDIF07_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CDIF07_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de pasivos ---

echo "Ruta pasivos"
echo "$RUTA_ORIGEN_PAS"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Pasivos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAS/TAP00201_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "TAP00201_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/TMP003_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TMP003_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de contingentes ---

echo "Ruta contingentes"
echo "$RUTA_ORIGEN_CON"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Contingentes" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_CON/SCTAF87_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "SCTAF87_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

echo "Revision terminada con exito"