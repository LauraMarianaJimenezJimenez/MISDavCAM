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
    *)
    VARIABLES+=("--var=$i ")
    shift
    ;;
esac
done
echo "Inicio llamado de ${ARCHIVO_SQL}"
echo "host de impala ${IMPALA_HOST}"
export PYTHON_EGG_CACHE=/tmp/impala-shell-python-egg-cache-${USER}
echo 'ruta python' $PYTHON_EGG_CACHE
#echo mJ2Kd7Wf | kinit  ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
#kinit -kt ${USUARIO_OOZIE}.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
kinit -kt hnbigdatasrv.keytab ${USUARIO_OOZIE}@${USUARIO_DOMINIO}
echo "VARIABLES"
echo "${VARIABLES[*]}"
impala-shell -k -i ${IMPALA_HOST} -f ${ARCHIVO_SQL} ${VARIABLES[*]}