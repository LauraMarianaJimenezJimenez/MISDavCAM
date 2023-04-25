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
    ruta_keytab=*)
    RUTA_KEYTAB="${i#*=}"
    shift
    ;;
    archivo_sql=*)
    ARCHIVO_SQL="${i#*=}"
    shift
    ;;
    *)
    VARIABLES+=("--var=$i ")
    shift
    ;;
esac
done
echo "Inicio llamado de ${ARCHIVO_SQL}"
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}
echo 'ruta python' $PYTHON_EGG_CACHE
#kinit -kt /home/countries/SV/SV_PRODUCCION/MIS/workflows/ITERACION_3/svbigdatasrv.keytab 
kdestroy
kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}
klist
#${USUARIO_OOZIE}@${USUARIO_DOMINIO}
#echo R3nslayer! | kinit ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
echo "VARIABLES"
echo "${VARIABLES[*]}"
impala-shell -k -i ${IMPALA_HOST} -f ${ARCHIVO_SQL} ${VARIABLES[*]}