#!/bin/bash
#
#    File:  Corr_Complex.sh 
#
#   About: 
#
#     Compute a correlation matrix result (pearsons) not using the SciDB 
#   pearson() operator, but instead a combination of other operators, most 
#   notably gemm(). 
#
#    This script is modified from Bryan L's script with the same intention. 
#
#------------------------------------------------------------------------------
#
if test $# -lt 3;then
cat << END
Usage:

Q3_Complex A B C

where, A and B are names of SciDB input matrices and C is the name of the
output.  The input matrices A and B are assumed to meet the following:
 - A and B must have the same number of rows
 - A and B must have a single, double-precision-valued attribute
The elements of C contain the correlations of every column of A with every
column of B. Compare the output of C with the R function cor(A,B), for example.

END
exit 1;
fi
#
#------------------------------------------------------------------------------
#
#  Some setup. 
ulimit -a
ulimit -aH

onhead_stopscidb.sh
onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

source /global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/Utils.sh
#
get_single_value_from_query() {
  local RESULT=$(iquery -o csv -p ${SCIDB_PORT_NUMBER} -q "${1}" | tail -1);
  # Remove any surrounding single quotes in the case of string results. This 
  # will not have any effect with numbers. 
  RESULT="${RESULT%\'}";
  RESULT="${RESULT#\'}";
  echo "${RESULT}";
}
#
purge() { 
  iquery -anq "remove(${CENTER_A_ARRAY_NAME})" 2>/dev/null
  iquery -anq "remove(${CENTER_B_ARRAY_NAME})" 2>/dev/null
  iquery -anq "remove(_COV)" 2>/dev/null
}
#------------------------------------------------------------------------------
#
A=$1    # Input matrix name
B=$2    # Input matrix name
C=$3    # Output matrix name
#
#------------------------------------------------------------------------------
#
#  NOTE: I could use the populate_dim_details_for_array infrastructure from 
#        the Utils directory, but instead I'll hard code it all in this 
#        script. 
#
# Dimension of input matrix A
#
echo "starting up"
CMD_COMPUTE_COUNT_ALL="
SELECT COUNT(*) FROM CRV_DATA
"
exec_aql_query_wd "${CMD_COMPUTE_COUNT_ALL}"

AN=$(get_single_value_from_query "SELECT ( high - low )  FROM dimensions ( ${A} ) WHERE No = 1");
ANM1=$((${AN} + 1))
echo "Number of COLUMNS of ${A} is ${AN}, and the length is ${ANM1}"
#
# Dimension of input matrix B
#
if [ "${A}" != "${B}" ]; then 
BN=$(get_single_value_from_query "SELECT ( high - low )  FROM dimensions ( ${B} ) WHERE No = 1");
BNM1=$((${BN} + 1))
else 
BN=${AN}
BNM1=${ANM1}
fi 
#
echo "Number of COLUMNS of ${B} is ${BN}, and the length is ${BNM1}"
#
# Number of rows (assumed to be the same for A and B):
M=$(get_single_value_from_query "SELECT ( high - low )  FROM dimensions ( ${A} ) WHERE No = 0");
NM1=$((${M} + 1))
#
echo "Number of ROWS of ${B} and ${A} is ${M}, so the length is ${NM1}"
#
# Attribute to use (matrix A) (take the first one)
valA=$(get_single_value_from_query "SELECT name FROM attributes ( ${A} ) WHERE No = 0");

echo "valA = ${valA} "

if [ "${A}" != "${B}" ]; then 
valB=$(get_single_value_from_query "SELECT name FROM attributes ( ${B} ) WHERE No = 0");
else
valB=${valA};
fi 
#
echo "Attribute from ${A} is ${A}.${valA}, attribute from ${B} is ${B}.${valB}"
#
iA=$(get_single_value_from_query "SELECT name FROM dimensions( ${A} ) WHERE No = 0");
jA=$(get_single_value_from_query "SELECT name FROM dimensions( ${A} ) WHERE No = 1");

if [ "${A}" != "${B}" ]; then 
iB=$(get_single_value_from_query "SELECT name FROM dimensions( ${A} ) WHERE No = 0");
jB=$(get_single_value_from_query "SELECT name FROM dimensions( ${A} ) WHERE No = 1");
else 
iB=${iA};
jB=${jA};
fi 
# 
echo "Dimension names for ${A} are ${A}.${iA} x ${A}.${jA}.";
echo "Dimension names for ${B} are ${B}.${iB} x ${B}.${jB}.";
#
# Schema for gemm build array
S="<${valA}:double>[${jA}=0:$AN,1000,0,${iB}=0:$BN,1000,0]"
# Repart schema for matrix A
As="<${valA}:double>[${iA}=0:$M,1000,0,${jA}=0:$AN,1000,0]"
As1="<${valA}:double>[${iA}=0:*,1000,0,${jA}=0:*,1000,0]"
# Repart schema for matrix B
Bs="<${valB}:double>[${iB}=0:$M,1000,0,${jB}=0:$BN,1000,0]"
Bs1="<${valB}:double>[${iB}=0:*,1000,0,${jB}=0:*,1000,0]"
# Repart schema for stdev aggregate
Asd="<s:double>[${jA}=0:$AN,1000,0]"
Bsd="<s:double>[${jB}=0:$BN,1000,0]"
#
echo " Schema for gemm build array is ${S}"
echo " Repart schema for matrix ${A} is ${As}"
echo "                               or ${As1}"
echo " Repart schema for matrix ${B} is ${Bs}"
echo "                               or ${Bs1}"
echo " Schemas for std aggregates is ${Asd}, and ${Bsd}"
#
#   NOTE: Need to work around a bug in the aggregates(...). It is not  
#         producing an array result of the correct size, but instead is 
#         padding the thing out to the array boundary. 
TA="subarray ( ${A}, 0, 0, ${M}, ${AN} )"
TB="subarray ( ${B}, 0, 0, ${M}, ${BN} )"
#
#------------------------------------------------------------------------------
#
#   Check that we've loaded the linear algebra stuff. . . 
iquery -naq "load_library('dense_linear_algebra-scidb')" >/dev/null
iquery -naq "remove(${C})" >/dev/null 2>&1
purge;
#
#
#==============================================================================
#
# OK, let's actually do something:
#
# Center the columns of the matrix A and B:
#
CENTER_A_ARRAY_NAME="_CENTER_A";
if [ "${A}" != "${B}" ]; then 
  CENTER_B_ARRAY_NAME="_CENTER_B";
else 
  CENTER_B_ARRAY_NAME="${CENTER_A_ARRAY_NAME}";
fi
#
iquery -naq "remove(${CENTER_A_ARRAY_NAME})" 2>/dev/null
iquery -naq "remove(${CENTER_B_ARRAY_NAME})" 2>/dev/null
iquery -naq "remove(_COV)" 2>/dev/null
#
#------------------------------------------------------------------------------
#
echo "Computing covariance matrix..."
echo "------------------------------"
#
# ORIGINAL QUERY
echo " 1.1: Reshaping the input matrix ${A} into ${CENTER_A_ARRAY_NAME} ...."
QUERY="
store(
  repart(
    substitute(
      project(
        apply(
          cross_join(
            ${TA} as _A,
            aggregate(
              ${TA}, 
              avg(${valA}) as mean, 
              ${jA}
            ) as _X, 
            _A.${jA}, 
            _X.${jA}
          ),
          _v, 
          ${valA}-mean
        ),
        _v
      ),
      build ( < _v : double > [${iA}=0:0,1,0],nan)
    ),
    ${As}
  ), 
  ${CENTER_A_ARRAY_NAME}
)"
#
#   We're going to;
#     1. Use TEMP on the ${CENTER_A_ARRAY_NAME}, and 
#     2. Pre-configure the array with a 1,000 x 1,000 "square" chunk 
#        configuration. 
#RUN THIS ONE
QUERY="
CREATE TEMP ARRAY ${CENTER_A_ARRAY_NAME} ${As1};
store(
  substitute(
    project(
      apply(
        cross_join(
          ${A} as _A,
          aggregate( ${A}, avg(${valA}) as mean, ${jA} ) as _X, 
          _A.${jA}, _X.${jA}
        ),
        _v, 
        ${valA}-mean
      ),
      _v
    ),
    build ( < _v : double > [${iA}=0:0,1,0],nan)
  ),
  ${CENTER_A_ARRAY_NAME}
)"
#
echo "${QUERY}"
time iquery -naq "${QUERY};"
#
#
if [ "${A}" != "${B}" ]; then 
  echo " 1.2: Reshaping the input matrix ${B} into ${CENTER_B_ARRAY_NAME} ...."

  QUERY="
store(
  repart(
    substitute(
      project(
        apply(
          cross_join(
            ${TB} as _B,
            aggregate (
              ${TB}, 
              avg(${valB}) as mean, 
              ${jB}
            ) as _X, 
            _B.${jB}, 
            _X.${jB}
          ),
          _v, 
          ${valB}-mean
        ),
        _v
      ),
      build( < _v : double > [${iB}=0:0,1,0],nan )
    ),
    ${Bs}
  ),
  ${CENTER_B_ARRAY_NAME}
)"
#
  QUERY="
CREATE TEMP ARRAY ${CENTER_B_ARRAY_NAME} ${Bs1};
store(
  substitute(
    project(
      apply(
        cross_join(
          ${TB} as _B,
          aggregate ( ${TB}, avg(${valB}) as mean, ${jB}) as _X, 
            _B.${jB}, _X.${jB}
        ),
        _v, 
        ${valB}-mean
      ),
      _v
    ),
    build( < _v : double > [${iB}=0:0,1,0],nan )
  ),
  ${CENTER_B_ARRAY_NAME}
)
"
  echo "${QUERY}"
  time iquery -naq "${QUERY};"
else 
  echo "Single input arrays ${A} ${B} so not reshaping ${B} ..."
fi 
#
#------------------------------------------------------------------------------
#
# Compute covariance matrix (center(A)^T * center(B))/(number of rows - 1):
echo " 2: Computing the covariance matrix ...."
QUERY="
store(
  substitute(
    project(
      apply(
        repart(
          sg(
            gemm(
              transpose(${CENTER_A_ARRAY_NAME}),
              ${CENTER_B_ARRAY_NAME}, 
              build(${S},0)
            ),
            1,-1
          ),
          < _v : double > [ ${jA}=0:${AN},1000,0,${iB}=0:$BN,1000,0] 
        ), 
        _v, 
        gemm/${M}
      ),
      _v
    ),
    build(<_v:double>[j=0:0,1,0],nan)
  ),
  _COV
)"
#
QUERY="
store(
  substitute(
    project(
      apply(
        repart(
          sg(
            gemm(
              transpose(${CENTER_A_ARRAY_NAME}),
              ${CENTER_B_ARRAY_NAME}, 
              build(${S},0)
            ),
            1,-1
          ),
          < _v : double > [ ${jA}=0:999,1000,0,${iB}=0:${BN},1000,0] 
        ), 
        _v, 
        gemm/${M}
      ),
      _v
    ),
    build(<_v:double>[j=0:0,1,0],nan)
  ),
  _COV
)"
#
echo "${QUERY}"
time iquery -naq "${QUERY}"
#
#------------------------------------------------------------------------------
#
# Scale resulting columns:
echo " 3: Computing result correlation matrix ${C}..."
SCALE="
project(
  apply(
    cross_join(
      sg(
        project(
          apply(
            cross_join(
              _COV as _W,
              repart(
                aggregate(
                  ${TB},
                  stdev(${valB}) as s,
                  ${jB}
                ),
                ${Bsd}
              ) as _X, 
              _W.${iB}, _X.${jB}
            ),
            u, _v/s
          ),
          u
        ), 
        1, -1
      ) as _X, 
      repart(
        aggregate(
          ${TA},
          stdev(${valA}) as s,
          ${jA}
        ), 
        ${Asd}
      ) as _Y,
      _X.${jA}, _Y.${jA}
    ),
    valA, u/s
  ),
  valA
)"
echo "store ( ${SCALE}, ${C})"
time iquery -anq "store ( ${SCALE}, ${C})"
#
#------------------------------------------------------------------------------
#
echo "------------------------------"
echo " Done. "

# iquery -anq "remove(${CENTER_A_ARRAY_NAME})" 2>/dev/null

# if [ "${A}" != "${B}" ]; then 
# # iquery -anq "remove(${CENTER_B_ARRAY_NAME})" 2>/dev/null
fi

# iquery -anq "remove(_COV)" 2>/dev/null

onhead_stopscidb.sh
