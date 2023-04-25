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
    ruta_fuentes_contabilidad=*)
    ruta_fuentes_contabilidad="${i#*=}"
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
export RUTA_DESTINO=${ruta_fuentes_contabilidad}
export RUTA_ORIGEN=${ruta_origen_rentabilidad}

#--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kdestroy
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
klist

#--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_CAF_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_CAF_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_PL_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BP_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BP_ACC.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_BL_ACC.csv $RUTA_DESTINO/MIS_PAR_REL_BL_ACC.csv

/*
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_CAF.csv $RUTA_DESTINO/MIS_PAR_CAT_CAF.csv
*/

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_PL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_PL.csv $RUTA_DESTINO/MIS_PAR_CAT_PL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BP.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BP.csv $RUTA_DESTINO/MIS_PAR_CAT_BP.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_BL.csv $RUTA_DESTINO/MIS_PAR_CAT_BL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_OFFI_MANUAL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_OFFI_MANUAL.csv $RUTA_DESTINO/MIS_PAR_CAT_OFFI_MANUAL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC.csv

/*
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BLCE_PROD_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_BLCE_PROD_LC.csv

fi

/*
hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
*/

if hdfs dfs -test -e $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv ; then

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_PL_ACC_LC.csv $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

else

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv
hdfs dfs -touchz $RUTA_DESTINO/MIS_HIERARCHY_PL_ACC_LC.csv

fi

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_BL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_BL.csv $RUTA_DESTINO/MIS_HIERARCHY_BL.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_HIERARCHY_UN.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_HIERARCHY_UN.csv $RUTA_DESTINO/MIS_HIERARCHY_UN.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_OFFI.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_OFFI.csv $RUTA_DESTINO/MIS_PAR_CAT_OFFI.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_EXPENSE.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_EXPENSE.csv $RUTA_DESTINO/MIS_PAR_CAT_EXPENSE.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_CAT_SECTOR_ECO.csv $RUTA_DESTINO/MIS_PAR_CAT_SECTOR_ECO.csv

#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TAX_ENG.csv
#hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TAX_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_TAX_ENG.csv

hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_REL_PROV_GL.csv
hdfs dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_REL_PROV_GL.csv $RUTA_DESTINO/MIS_PAR_REL_PROV_GL.csv

#Tabla extra: MIS_LOAD_VPSPLASTICO 
#hdfs dfs -rm -skipTrash $RUTA_DESTINO/MIS_LOAD_VPSPLAST.csv
#hdfs dfs -cp $RUTA_ORIGEN/VPSPLAST_$YEAR$MONTH$DAY.csv $RUTA_DESTINO/MIS_LOAD_VPSPLAST.csv