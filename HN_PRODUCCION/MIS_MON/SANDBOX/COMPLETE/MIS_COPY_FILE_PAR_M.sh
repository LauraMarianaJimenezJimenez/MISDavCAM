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
    ruta_origen_clientes=*)
    ruta_origen_clientes="${i#*=}"
    shift
    ;;
    ruta_fuentes_activos=*)
    ruta_fuentes_activos="${i#*=}"
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

--- Impresión de verificación de Fecha ---
echo "Fecha: $D"
echo "Day: $DAY"
echo "Month: $MONTH"
echo "Year: $YEAR"

--- Creación de variables de ruta origen y destino para el proceso de copia ---
export RUTA_ORIGEN_PAR=${ruta_origen_parametria}
export RUTA_ORIGEN_CLIENTES=${ruta_origen_clientes}
export RUTA_DESTINO=${ruta_fuentes_activos}

--- Creación de ruta de variable PYTHON EGG CACHE, necesaria para la ejecución de acciones en HUE ---
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}

--- Solicitud del Ticket de Kerberos para autenticación con el sistema ---
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}

--- Ejecución de la acción de copia de archivos, removiendo primero el archivo si existe ---
hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_ALLOC_AC_ENG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_ALLOC_AC_ENG.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_ALLOC_SEG_ENG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_ALLOC_SEG_ENG.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_ALLOC_AC_DRI.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_AC_DRI_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_ALLOC_AC_DRI.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_ALLOC_SEG_DRI.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_ALLOC_SEG_DRI_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_ALLOC_SEG_DRI.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_TAX_ENG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_TAX_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_TAX_ENG.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_INTER_SEG_ENG.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_ENG_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_INTER_SEG_ENG.csv

hadoop dfs -rm -skipTrash $RUTA_DESTINO/MIS_PAR_INTER_SEG_DRI.csv
hadoop dfs -cp $RUTA_ORIGEN_PAR/MIS_PAR_INTER_SEG_DRI_$YEAR$MONTH.csv $RUTA_DESTINO/MIS_PAR_INTER_SEG_DRI.csv