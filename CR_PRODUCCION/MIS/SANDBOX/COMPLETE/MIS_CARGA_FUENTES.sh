#----- PROCESO DE COPIA DE ARCHIVOS FUENTE A CARPETAS PROPIAS DEL MIS -----

#--- Impresión en Consola del nombre del proceso ---
echo 'Inicio copia de archivos de Contabilidad'

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
    ruta_origen_contabilidad=*)
    ruta_origen_contabilidad="${i#*=}"
    shift
    ;;
    ruta_origen_activos=*)
    ruta_origen_activos="${i#*=}"
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
    ruta_origen_rof=*)
    ruta_origen_rof="${i#*=}"
    shift
    ;;
    ruta_origen_contingentes=*)
    ruta_origen_contingentes="${i#*=}"
    shift
    ;;
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
    shift
    ;;
    ruta_origen_curvas=*)
    ruta_origen_curvas="${i#*=}"
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

#--- Definición de variables de Fecha ---
D=${PERIODO}
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
export RUTA_ORIGEN_CONT=${ruta_origen_contabilidad}
export RUTA_ORIGEN_ACT=${ruta_origen_activos}
export RUTA_ORIGEN_ROF=${ruta_origen_rof}
export RUTA_ORIGEN_CLI=${ruta_origen_clientes}
export RUTA_ORIGEN_PAS=${ruta_origen_pasivos}
export RUTA_ORIGEN_CON=${ruta_origen_contingentes}
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_ORIGEN_CUR=${ruta_origen_curvas}
export RUTA_DESTINO=${ruta_destino}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
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

hadoop dfs -test -e $RUTA_ORIGEN_CONT/cb_balcobx_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "cb_balcobx_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CONT/BAL_SEGUROS.CSV
if [ $? -ne 0 ] ; then
echo "BAL_SEGUROS.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de activos ---

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Activos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Ruta Activos"
echo "$RUTA_ORIGEN_ACT"

hadoop dfs -test -e $RUTA_ORIGEN_ACT/Cainfoper_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Cainfoper_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/InfoTrabajo_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "InfoTrabajo_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/ListaMov_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "ListaMov_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/TarEnte_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "TarEnte_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/TCS_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "TCS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/LCS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "LCS_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_A2.csv
if [ $? -ne 0 ] ; then
echo "MV_$D_A2.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_BANX.csv
if [ $? -ne 0 ] ; then
echo "MV_$D_BANX.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_MC.csv
if [ $? -ne 0 ] ; then
echo "MV_$D_MC.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_ONE.csv
if [ $? -ne 0 ] ; then
echo "MV_$D_ONE.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_A1.csv
if [ $? -ne 0 ] ; then
echo "MV_$D._A1CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/MV_${YEAR}${MONTH}${DAY}_DEBT.csv
if [ $? -ne 0 ] ; then
echo "MV_$D_DEBT.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/TarjetasEntregadas_${YEAR}${MONTH}${DAY}.csv
if [ $? -ne 0 ] ; then
echo "TarjetasEntregadas_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/Subproductos_${YEAR}${MONTH}${DAY}.csv
if [ $? -ne 0 ] ; then
echo "Subproductos_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/Interes_Operacion_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Interes_Operacion_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_ACT/Auxiliar_Inversiones_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Auxiliar_Inversiones_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes de Clientes ---

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Clientes" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Ruta Clientes"
echo "$RUTA_ORIGEN_CLI"

hadoop dfs -test -e $RUTA_ORIGEN_CLI/cl_produc_cli_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "cl_produc_cli_$YEAR$MONTH$DAY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes pasivos ---

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Pasivos" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Ruta pasivos"
echo "$RUTA_ORIGEN_PAS"


hadoop dfs -test -e $RUTA_ORIGEN_PAS/Estado_de_Portafolio_118_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Estado_de_Portafolio_118_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/Estado_de_Portafolio_119_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Estado_de_Portafolio_119_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ahbkr001CD_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "ahbkr001CD_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ahbkr001CM_$YEAR$MONTH$DAY.csv  
if [ $? -ne 0 ] ; then
echo "ahbkr001CM_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ccbkr001_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "ccbkr001_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ahretencion_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "ahretencion_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/ccretencion_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "ccretencion_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/mbalance_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "mbalance_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/cuvenore_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "cuvenore_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/Estado_de_Portafolio_100_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Estado_de_Portafolio_100_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/Estado_de_Portafolio_87_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "Estado_de_Portafolio_87_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAS/Estado_de_Portafolio_88_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Estado_de_Portafolio_88_$D.CSV" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes contingentes ---

echo "Ruta contingentes"
echo "$RUTA_ORIGEN_CON"


--- Validacion fuentes parametria ---

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Parametria" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Ruta parametria"
echo "$RUTA_ORIGEN_PAR"

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ENTITY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ENTITY.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

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

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAF_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_OPER.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BP_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BL_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_REG_DIMENSIONS.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_REG_DIMENSIONS.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CONV.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CONV.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ECO_GRO.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MUL_LAT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_MUL_LAT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_SPE_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_ENG_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_ENG_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_AC_DRI_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_ENG_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_ALLOC_SEG_DRI_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_EXP_TYP.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_EXP_TYP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_EXP_TYP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_INTER_SEG_DRI_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_ENG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_BALAN_SEG_ENG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_BALAN_SEG_DRI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_BALAN_SEG_DRI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_OFFI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_OFFI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ACCO_CENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ACCO_CENT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_EXPENSE.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_EXPENSE.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_INST_CLI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_INST_CLI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_SECTOR_ECO.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_MANAGER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_MANAGER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PRODUCT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PRODUCT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SUBPRODUCT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_SUBPRODUCT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PROD_BL.csv 
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PROD_BL.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_TAX_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TAX_ENG_$D.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Tasas de Cambio" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt


hadoop dfs -test -e $RUTA_ORIGEN_CUR/TipoCambio_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "TipoCambio_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROG_CARD.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PROG_CARD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Fuentes TTI" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_PAR/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CURVES_$D.csv " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_Captacion_Col_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Curva_Captacion_Col_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_Captacion_Dol_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Curva_Captacion_Dol_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_Colocacion_Col_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "Curva_Colocacion_Col_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_Colocacion_Dol_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "Curva_Colocacion_Dol_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_PIPCA_Col_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "Curva_PIPCA_Col_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_CUR/Curva_PIPCA_Dol_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "Curva_PIPCA_Dol_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

echo "echo fin"