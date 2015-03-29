#!/bin/bash
# 
#   File:   Benchmark/Graph/schema/Utils.sh 
#
#  About: 
#
#    This script implements a long list of functions used across all of the 
#   other scripts. 
#
#
###############################################################################
#
#  Utility Functions: 
#
#------------------------------------------------------------------------------
#
#  Adjust these values according to how SciDB is set up on your local box. 
# set -x 
#
module load scidb/14.8

export SCIDB_PORT_NUMBER=1239
# export BENCHMARK_HOME=/global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/
#
#  Useful shell script functions for interacting with SciDB. 
#
exec_afl_query() {
    iquery -o csv -p ${SCIDB_PORT_NUMBER} ${2} -aq "${1};"
};
#
exec_aql_query() {
    iquery -o csv -p ${SCIDB_PORT_NUMBER} ${2} -q "${1};"
};
#
exec_afl_query_wd() {
    echo "Query: ${1}"
    time -f "Elapsed Time: %E" 
    iquery -o dcsv -p ${SCIDB_PORT_NUMBER} ${2} -aq "${1}";
};
#
exec_aql_query_wd() {
    echo "Query: ${1}"
    time -f "Elapsed Time: %E" 
    iquery -o dcsv -p ${SCIDB_PORT_NUMBER} ${2} -q "${1}";
};
#
#------------------------------------------------------------------------------
#
#  Useful shell functions for error handling, tracing, etc. 
# 
VERBOSE_LEVEL=3;
#
i_echo() {
  I_ECHO_STR="";
  I_ECHO_ARG_CNT="$#";
  if [ "${I_ECHO_ARG_CNT}" -gt "1" ]; then
    I_ECHO_ARG_NUM=1;
    for S in $@; do
      if [ "${I_ECHO_ARG_NUM}" -eq "$#" ]; then
        if [ "${VERBOSE_LEVEL}" -gt "${S}" ]; then
          printf "${I_ECHO_STR}\n";
        fi
      else
        I_ECHO_STR="${I_ECHO_STR} ${S}";
      fi
      I_ECHO_ARG_NUM=$((I_ECHO_ARG_NUM + 1));
    done
  elif [ "${I_ECHO_ARG_CNT}" -eq "1" ]; then
    if [ "${VERBOSE_LEVEL}" -gt "${1}" ]; then
      printf "\n";
    fi
  fi
};
#
v_echo() {
  V_ECHO_STR="";
  for VS in $@; do
    V_ECHO_STR="${V_ECHO_STR} ${VS}";
  done
  i_echo ${V_ECHO_STR};
};
#
error_usage_exit() { 
  echo "error_usage_exit called with $# args -> $@";

  echo "###############################################################################";
  echo "";
  echo "	${1} ERROR:";

  echo "";
  echo "------------------------------------------------------------------------------";
  echo "";
  case ${2} in 

1)
  echo ">>>>>  Less than ${3} required arguments <<<<<"
;;

2)
  echo ">>>>>  More than ${3} required arguments <<<<<"
;;

3)
  echo ">>>>>  Array Named '${3}' NOT FOUND <<<<<"
;;

4)
  echo ">>>>>  Array Named '${3}' has ${4} dimensions ... can only have 2 <<<<<"
;;

5)
  echo ">>>>>  Array Named '${3}' does not have float attribute named '${4}' <<<<<"
;;

6)
  echo ">>>>>  Length of dimension 0 in Array Named '${3}' (${4}) doesn't equal length of dimension 2 (${5}) <<<<<"
;;

7)
  echo ">>>>>  Vertex ${4} not in dimensions of Array Named '${3}' <<<<<"
;;

7)
  echo ">>>>>  compute_iteration called with $@ <<<<<"
;;

*)
  echo ">>>>>  UNKNOWN ERROR NUMBER ${2} <<<<<<"
;;

  esac

  echo "";

  usage ${1};

  exit;
};

#
#------------------------------------------------------------------------------
#
#  Shell functions for pulling information out about the shape / scope of 
#  the problem space. 
#
check_array_exists() { 
  local ARRAY_CNT=$(exec_aql_query "SELECT COUNT(*) FROM list('arrays') WHERE name = '${1}'" | tail -1);
  echo "${ARRAY_CNT}";
};
#
get_adjacency_graph_dimension_count() { 
  local DIM_CNT=$(exec_aql_query "SELECT COUNT(*) FROM dimensions(${1})" | tail -1);
  echo "${DIM_CNT}";
};
#
get_array_attributes_matching() { 
  local ATTR_CNT=$(exec_aql_query "SELECT COUNT(*) FROM attributes(${1}) WHERE regex(name, '${2}' ) AND type_id='float'" | tail -1);
  echo "${ATTR_CNT}";
};
#
get_max_attribute_in_array() {
  local ATTR_MAX=$(exec_aql_query "SELECT max ( ${2} ) FROM ${1}" | tail -n1);
  echo "${ATTR_MAX}";
};
#
get_single_value() { 
  local RESULT=$(exec_aql_query "${1}" | tail -1);
  echo "${RESULT}";
}
#
#  NOTE: These are useful macros that allow me to pull out quick information
#        concerning the way arrays are using chunking.
#
CMD_LOAD_CHUNK_MAP_MACRO_MODULE="
load_module ('/global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/macros.txt')
"
exec_afl_query "${CMD_LOAD_CHUNK_MAP_MACRO_MODULE}" > /dev/null 
#
#  Load the linear algebra library. This is used for the spgemm(...) operaor. 
#
exec_afl_query "load_library('linear_algebra-scidb')" > /dev/null # for spgemm()
#exec_afl_query "load_library('dense_linear_algebra-p4')" # for gemm
#
#------------------------------------------------------------------------------
#
# Variables used to hold an array's dimension information. 
declare -a ADDG_ARRAY_DIMS_NAMES;
declare -a ADDG_ARRAY_DIMS_CL;
declare -a ADDG_ARRAY_DIMS_MINS;
declare -a ADDG_ARRAY_DIMS_MAXS;

#
#  Function for testing. . . 
reset_dim_details_for_array() {
  unset ADDG_ARRAY_DIMS_NAMES;
  unset ADDG_ARRAY_DIMS_CL;
  unset ADDG_ARRAY_DIMS_MINS;
  unset ADDG_ARRAY_DIMS_MAXS;
}

#
#  Function used to populate the per-array dimension variables. 
populate_dim_details_for_array() {
  reset_dim_details_for_array;
  local ATTR_CNT=$(exec_aql_query "
SELECT No, name, chunk_interval, low, high 
  FROM dimensions(${1})" | sed 1d);
  #  echo "ATTR_CNT = ${ATTR_CNT}";
  for dim_line in ${ATTR_CNT}; do 
    #  echo "dim_line = ${dim_line}";
    local ARRAY_DIM_INDEX=(${dim_line//,/ });
    #  echo "ARRAY_DIM_INDEX=${ARRAY_DIM_INDEX}";
    ADDG_ARRAY_DIMS_NAMES[${ARRAY_DIM_INDEX}]=${ARRAY_DIM_INDEX[1]//\'/};
    #  echo "NAME = ${ARRAY_DIM_INDEX[1]//\'/}";
    ADDG_ARRAY_DIMS_CL[${ARRAY_DIM_INDEX}]=${ARRAY_DIM_INDEX[2]};
    #  echo "CL = ${ARRAY_DIM_INDEX[2]}";
    ADDG_ARRAY_DIMS_MINS[${ARRAY_DIM_INDEX}]=${ARRAY_DIM_INDEX[3]};
    #  echo "DIM_MIN = ${ARRAY_DIM_INDEX[3]}";
    ADDG_ARRAY_DIMS_MAXS[${ARRAY_DIM_INDEX}]=${ARRAY_DIM_INDEX[4]};
    #  echo "DIM_MAX = ${ARRAY_DIM_INDEX[4]}";
  done
};
#
#  Function to dump the per-array dimension variable information. 
dump_dim_details() { 
  v_echo "-------------------------------------------------------------------------------" 0;
  v_echo "   Dimension Details: " 0;
  v_echo "\t ARRAY.DIM[0] = ${ADDG_ARRAY_DIMS_NAMES[0]} ${ADDG_ARRAY_DIMS_MINS[0]} TO ${ADDG_ARRAY_DIMS_MAXS[0]} CL=${ADDG_ARRAY_DIMS_CL[0]}" 0;
  v_echo "\t ARRAY.DIM[1] = ${ADDG_ARRAY_DIMS_NAMES[1]} ${ADDG_ARRAY_DIMS_MINS[1]} TO ${ADDG_ARRAY_DIMS_MAXS[1]} CL=${ADDG_ARRAY_DIMS_CL[1]}" 0;
  v_echo "-------------------------------------------------------------------------------" 0;
};

#
#  Testing ... 
#  populate_dim_details_for_array CALLS
#  dump_dim_details
#
#------------------------------------------------------------------------------
#
#   Core function. This implements an iterative structure over three query 
#  arguments. The first is the INIT query, which sets up the STATE. The 
#  second is the ITER method, and the last is the CHECK. There is an optional 
#  fourth argument that limits the total number of steps the iterator can 
#  execute. 
compute_iteration() {

  if [ "$#" -lt 3 ]; then 
    error_usage_exit ${0} 8 $@;
  fi

  v_echo "+--------+" 0;
  v_echo "|  INIT  |" 0;
  v_echo "+--------+" 0;

  exec_aql_query "${1}" -n > /dev/null;
  local CHECK_RESULT=0;
  local STEP_CNT=0;
  local MAX=4294967296;

  if [ -n "${4}" ]; then 
    local MAX=${4};
  fi 

  until [[ "${CHECK_RESULT}" -eq "1" || "${STEP_CNT}" -ge "${MAX}" ]]; do

    D=`date`;

    v_echo "+-------------------+" 0;
    v_echo "| ITER ${STEP_CNT} (MAX = ${MAX}) |" 0;
    v_echo "|   ${D}  |" 0;
    v_echo "+-------------------+" 0;
    exec_aql_query "${2}" -n > /dev/null;

    v_echo "+---------+" 0;
    v_echo "|  CHECK  |" 0;
    v_echo "+---------+" 0;
    local CHECK_RESULT=$(exec_aql_query "${3}" | tail -n +2 | head -1);

    v_echo "+-------------------+" 0;
    v_echo "| CHECK_RESULT = ${CHECK_RESULT} |" 0;
    v_echo "+-------------------+" 0;
    local STEP_CNT=$(($STEP_CNT + 1));

  done 
}
#
#
f_compute_iteration() {

  if [ "$#" -lt 3 ]; then
    error_usage_exit ${0} 8 $@;
  fi

  v_echo "+--------+" 0;
  v_echo "|  INIT  |" 0;
  v_echo "+--------+" 0;
  
  exec_afl_query "${1}" -n > /dev/null;
  local CHECK_RESULT=0;
  local STEP_CNT=0;
  local MAX=4294967296;

  if [ -n "${4}" ]; then
    local MAX=${4};
  fi

  until [[ "${CHECK_RESULT}" -eq "1" || "${STEP_CNT}" -ge "${MAX}" ]]; do

    v_echo "+-------------------+" 0
    v_echo "| ITER ${STEP_CNT} (MAX = ${MAX}) |" 0 
    v_echo "+-------------------+" 0
    exec_afl_query "${2}" -n > /dev/null;

    v_echo "+---------+" 0;
    v_echo "|  CHECK  |" 0;
    v_echo "+---------+" 0;
    local CHECK_RESULT=$(exec_afl_query "${3}" | tail -n +2 | head -1);

    v_echo "+-------------------+" 0;
    v_echo "| CHECK_RESULT = ${CHECK_RESULT} |" 0;
    v_echo "+-------------------+" 0;
    local STEP_CNT=$(($STEP_CNT + 1));

  done
}
#
# INIT_ONE="DROP ARRAY STATE; SELECT MAX(0) AS CNT INTO STATE FROM CALLS"
# INIT="SELECT MAX(0) AS CNT INTO STATE FROM CALLS"
# ITER="UPDATE STATE SET CNT = CNT + 1"
# CHECK="SELECT iif(S>=5,1,0) AS R FROM ( SELECT SUM ( CNT ) AS S FROM STATE )"
#
# compute_iteration "${INIT}" "${ITER}" "${CHECK}"
# iquery -q "SELECT * FROM STATE;"
# compute_iteration "${INIT}" "${ITER}" "${CHECK}" 3
# iquery -q "SELECT * FROM STATE;"
# 
#
###############################################################################
