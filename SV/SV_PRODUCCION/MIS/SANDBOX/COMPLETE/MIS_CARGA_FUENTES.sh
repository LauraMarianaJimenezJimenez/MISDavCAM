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
    ruta_origen_parametria=*)
    ruta_origen_parametria="${i#*=}"
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
NEXT=$(date -d "${PERIODO} +1 day" +%d)

#--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

#--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_ACTIVOS=${ruta_origen_activos}
export RUTA_ORIGEN_RENTABILIDAD=${ruta_origen_rentabilidad}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_ORIGEN_PASIVOS=${ruta_origen_pasivos}
export RUTA_ORIGEN_PARAMETRIA=${ruta_origen_parametria}
export RUTA_DESTINO=${ruta_destino}


#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}


--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---

hadoop dfs -rm -skipTrash $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
hdfs dfs -touchz $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

echo "Faltan:" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt


--- Validacion fuentes de contabilidad ---

echo "Ruta rentabilidad"
echo "$RUTA_ORIGEN_RENTABILIDAD"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Contabilidad" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/GLMST_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "GLMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/GLBLN_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "GLBLN_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/WFSALDOS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "WFSALDOS_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/SALDOSCON_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "SALDOSCON_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

if [ $NEXT -eq 01 ] ; then
   hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/Carga_Manual_Valores_$YEAR$MONTH$DAY.csv
   if [ $? -ne 0 ] ; then
   echo "Carga_Manual_Valores_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
   else
   echo "El archivo existe"
   fi
else
echo "El archivo no es necesario"
fi

--- Validacion fuentes parametria ---

echo "Ruta parametria"
echo "$RUTA_ORIGEN_PARAMETRIA"

echo " " | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
echo "Parametria" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_CAF_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAF_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_BP_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BP_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_BL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_CAF.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CAF.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_PL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_BP.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_OFFI_MANUAL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_OFFI_MANUAL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_BLCE_PROD.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_PL_ACC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_BLCE_PROD_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BLCE_PROD_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_PL_ACC_LC.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PL_ACC_LC.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_UN.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_UN.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_OFFI.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_OFFI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_ACCO_CENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_OFFI.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_ACCO_CENT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ACCO_CENT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_EXPENSE.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_EXPENSE.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_SECTOR_ECO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_SECTOR_ECO.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_PROV_GL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PROV_GL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes activos  ---

echo "Ruta activos"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/DEALS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "DEALS_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/VPSCUENT_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "VPSCUENT_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/VPSATSMP_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "VPSATSMP_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/WFCUMST_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "WFCUMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/VPSPLAN_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "VPSPLAN_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/GLSOB_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "GLSOB_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/SOBDT_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "SOBDT_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CSEXTMSTF_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CSEXTMSTF_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CSPLNINTF_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CSPLNINTF_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/DLAHI_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "DLAHI_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CREMST_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CREMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/ACMST_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "ACMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes clientes  ---
echo "Ruta clientes"
echo "$RUTA_ORIGEN_RENTABILIDAD"


hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CUMST_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CUMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes gastos  ---
echo "Ruta gastos"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/TRANS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TRANS_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/TRANSAJU_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TRANSAJU_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes comisiones  ---
echo "Ruta coisiones"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/TRANS_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "TRANS_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CSD533GLF_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CSD533GLF_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes pasivos  ---
echo "Ruta coisiones"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CNTRLRTE_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CNTRLRTE_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes fuera del balance  ---
echo "Ruta coisiones"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/LCMST_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "LCMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes fuera del balance  ---
echo "Ruta coisiones"
echo "$RUTA_ORIGEN_RENTABILIDAD"

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/LCMST_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "LCMST_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

--- Validacion fuentes provisiones  ---
echo "Ruta comisiones"
echo "$RUTA_ORIGEN_RENTABILIDAD"


hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/WFDLRES_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "WFDLRES_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi


--- Validacion fuentes operacional  ---
echo "Ruta parametria operacional"
echo "$RUTA_ORIGEN_PARAMETRIA"

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_SUBPROD_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_SUBPROD_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_CAF_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_CAF_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_BL_OPER.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BL_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CURVES_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CURVES_$D" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_SUBPRODUCT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_SUBPRODUCT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_PRODUCT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PRODUCT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_PROG_CARD.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_PROG_CARD.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_TTI_ENG_$YEAR$MONTH.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_ENG_$YEAR$MONTH.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_TTI_SPE_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_TTI_SPE_$D.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_BP_OPER.csv 
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_BP_OPER.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_EXP_TYP.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_EXP_TYP.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_REG_DIMENSIONS.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_REG_DIMENSIONS.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_MUL_LAT.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_MUL_LAT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_ECO_GRO.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_MUL_LAT.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_CONV.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_CONV.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_CAT_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_CAT_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_ECO_GRO_REG.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_ECO_GRO_REG.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_COM_GL.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_COM_GL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_PAR_REL_PROD_SPE.csv
if [ $? -ne 0 ] ; then
echo "MIS_PAR_REL_PROD_SPE.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_PROD_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PROD_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_PARAMETRIA/MIS_HIERARCHY_PROD_BL.csv
if [ $? -ne 0 ] ; then
echo "MIS_HIERARCHY_PROD_BL.csv" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CNTRLBRN_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CNTRLBRN_$D" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CCDSC_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "CCDSC_$D" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/CNOFC_$YEAR$MONTH$DAY.csv 
if [ $? -ne 0 ] ; then
echo "CNOFC_$D" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi

hadoop dfs -test -e $RUTA_ORIGEN_RENTABILIDAD/RATES_$YEAR$MONTH$DAY.csv
if [ $? -ne 0 ] ; then
echo "RATES_$D" | hadoop fs -appendToFile - $RUTA_DESTINO/$FECHA$"_FUENTES_FALTANTES_"$D.txt
else
echo "El archivo existe"
fi