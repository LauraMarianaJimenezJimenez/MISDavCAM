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
    ruta_fuentes_completo=*)
    ruta_fuentes_completo="${i#*=}"
    shift
    ;;
    ruta_origen_curvas=*)
    ruta_origen_curvas="${i#*=}"
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

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_fuentes_completo}
export RUTA_ORIGEN_CUR=${ruta_origen_curvas}

echo "RUTA_DESTINO: $RUTA_DESTINO"

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_ENTITY.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_ENTITY.csv $RUTA_DESTINO/MIS_PAR_ENTITY.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv

/*
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

fi

/*
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_BL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_UN.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_UN.csv $RUTA_DESTINO/MIS_HIERARCHY_UN.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CAF.csv $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL.csv $RUTA_DESTINO/MIS_PAR_CAT_PL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BP.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BP.csv $RUTA_DESTINO/MIS_PAR_CAT_BP.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv $RUTA_DESTINO/MIS_PAR_CAT_BL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BP_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv $RUTA_DESTINO/MIS_PAR_REL_BL_OPER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv $RUTA_DESTINO/MIS_PAR_REL_REG_DIMENSIONS.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv $RUTA_DESTINO/MIS_PAR_CAT_CONV.csv

/*
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO.csv

fi

/*
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_MUL_LAT.csv

fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
echo $?

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_TTI_SPE.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_TTI_ENG.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv $RUTA_DESTINO/MIS_PAR_REL_EXP_TYP.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_BALAN_SEG_ENG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_ENG.csv $RUTA_DESTINO/MIS_PAR_BALAN_SEG_ENG.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_BALAN_SEG_DRI.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_DRI.csv $RUTA_DESTINO/MIS_PAR_BALAN_SEG_DRI.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_OFFI.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_OFFI.csv $RUTA_DESTINO/MIS_PAR_CAT_OFFI.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ACCO_CENT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ACCO_CENT.csv $RUTA_DESTINO/MIS_PAR_CAT_ACCO_CENT.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_EXPENSE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_EXPENSE.csv $RUTA_DESTINO/MIS_PAR_CAT_EXPENSE.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_INST_CLI.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_INST_CLI.csv $RUTA_DESTINO/MIS_PAR_CAT_INST_CLI.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_REL_ECO_GRO_REG.csv

/*
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_PAR_CAT_ECO_GRO_REG.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_MANAGER.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MANAGER.csv $RUTA_DESTINO/MIS_PAR_CAT_MANAGER.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PRODUCT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PRODUCT.csv $RUTA_DESTINO/MIS_PAR_CAT_PRODUCT.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SUBPRODUCT.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SUBPRODUCT.csv $RUTA_DESTINO/MIS_PAR_CAT_SUBPRODUCT.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_PROD_BL.csv

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

hdfs dfs -rm -skipTrash $RUTA_DESTINO/EXCH_RATE.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/TipoCambio_$YEAR$MONTH.csv $RUTA_DESTINO/EXCH_RATE.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PROG_CARD.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROG_CARD.csv $RUTA_DESTINO/MIS_PAR_REL_PROG_CARD.csv


if [ "${incluir_fuentes_tardias}" = "SI" ]; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_CAPTACION_COL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_Captacion_Col_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_CAPTACION_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_CAPTACION_DOL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_Captacion_Dol_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_CAPTACION_DOL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_COLOCACION_COL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_Colocacion_Col_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_COLOCACION_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_COLOCACION_DOL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_Colocacion_Dol_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_COLOCACION_DOL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_PIPCA_COL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_PIPCA_Col_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_PIPCA_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_PIPCA_DOL.csv
hdfs dfs -cp $RUTA_ORIGEN_CUR/Curva_PIPCA_Dol_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/CURVA_PIPCA_DOL.csv

else 

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_CAPTACION_COL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_CAPTACION_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_CAPTACION_DOL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_CAPTACION_DOL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_COLOCACION_COL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_COLOCACION_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_COLOCACION_DOL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_COLOCACION_DOL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_PIPCA_COL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_PIPCA_COL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/CURVA_PIPCA_DOL.csv
hdfs dfs -touchz $RUTA_DESTINO/CURVA_PIPCA_DOL.csv

fi