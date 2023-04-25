#----- PROCESO DE COPIA DE ARCHIVOS DE PARAMETRIA CONTABLE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de parametria operacionales'

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
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
    shift
    ;;
    ruta_origen_rentabilidad=*)
    ruta_origen_rentabilidad="${i#*=}"
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
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_activos}
export RUTA_ORIGEN_RENTABILIDAD=${ruta_origen_rentabilidad}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kdestroy
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
klist

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_SUBPROD_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_SUBPROD_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_SUBPROD_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_CURVES.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SUBPRODUCT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SUBPRODUCT.csv $RUTA_DESTINO/MIS_PAR_CAT_SUBPRODUCT.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PRODUCT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PRODUCT.csv $RUTA_DESTINO/MIS_PAR_CAT_PRODUCT.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PROG_CARD.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PROG_CARD.csv $RUTA_DESTINO/MIS_PAR_CAT_PROG_CARD.csv

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_CURVES.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv

else

echo "El archivo de curvas para el dia de ejecucion no se encontro, se replica el ultimo cargado"
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CURVES.csv
hdfs dfs -cp $RUTA_DESTINO/MIS_PAR_CURVES_BK.csv $RUTA_DESTINO/MIS_PAR_CURVES.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv

/*
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv
 
if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_COM_GL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_COM_GL.csv $RUTA_DESTINO/MIS_PAR_COM_GL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PROD_SPE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROD_SPE.csv $RUTA_DESTINO/MIS_PAR_REL_PROD_SPE.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CNTRLBRN.csv
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CNTRLBRN_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CNTRLBRN.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CCDSC.csv
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CCDSC_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CCDSC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CNOFC.csv
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/CNOFC_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CNOFC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/RATES.csv
hdfs dfs -cp $RUTA_ORIGEN_RENTABILIDAD/RATES_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/RATES.csv