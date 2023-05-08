VARIABLES=( )
for i in "$@" 
do
case $i in
    impala_host=*)
    IMPALA_HOST="${i#*=}"
    shift
    ;;
    usuario_oozie=*)
    USUARIO_OOZIE="${i#*=}"
    shift
    ;;
    usuario_dominio=*)
    USUARIO_DOMINIO="${i#*=}"
    shift
    ;;
    archivo_sql=*)
    ARCHIVO_SQL="${i#*=}"
    shift
    ;;
    periodo=*)
    PERIODO="${i#*=}"
    shift
    ;;
    *)
    VARIABLES+=("--var=$i ")
    shift
    ;;
esac
done

--- Definición de variables de Fecha ---
D=${PERIODO}
DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

echo "periodo1 $D"
echo "ano $YEAR"
echo "mes $MONTH"

dateTemp=$(date -d "${YEAR}${MONTH}01 + 1 month")
echo "tmp $dateTemp"
D=$(date -d "$dateTemp - 1 day" "+%Y%m%d")

echo "periodo $D"
periodo+=("--var=periodo=$D ")

echo "Inicio llamado de ${ARCHIVO_SQL}"
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}
echo 'ruta python' $PYTHON_EGG_CACHE
#echo R3nslayer! | kinit  ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
#kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
kinit -kt svbigdatasrv.keytab ${USUARIO_OOZIE}
klist
echo "VARIABLES"
echo "${VARIABLES[*]}"
impala-shell -k -i ${IMPALA_HOST} -f ${ARCHIVO_SQL} ${periodo} ${VARIABLES[*]}