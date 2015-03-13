#!/bin/bash
# 
#   File:   Benchmark/Graph/schema/Q3.sh 
#
#  About: 
#
#    The specification of Q3 from the benchmark is: 
#
#  " Q3: Find all pairs of nodes u and v such that there is a high overlap 
#    between their neighborhoods. This is a simple way to do “link prediction”,
#    i.e., identify potentially missing links in the network, and it can also 
#    be used for “entity resolution”, i.e., deciding two phone numbers belong
#    to the same user. Specifically, ignoring the directionality of the 
#    edges, find the pairs of nodes (u, v) such that: Jaccard 
#    Coefficient(N(u), N(v)) = |N(u) U N(v)| / |N(v) n N(u)| > 0.9. 
#    Here N(u) denotes the neighboring nodes of u."
#
#  So ... 
#
#   1. The starting point is an array C[n x n], being an adjacency matrix 
#      reflecting a phone call < from, to > log. Worth noting that the OP
#      calls for a 'neighborhood', which might simply be the first hop 
#      out region, or the kth. For now, let's go with 1-hop, which means we're
#      dealing with the original array. 
#   
#   2. The goal is to compute a matrix JD[n x n] containing all pairs of 
#      Jaccard Distances (Jaccard Coefficients) JD[u,v] where u and v are 
#      drawn from n. (The further, trivial step is to filter them). 
# 
#      Each JD[u,v] cell contains: 
#
#                        | Nk[u] INTERSECT Nk[v] |
#        JD[u,v]  =    -----------------------------
#                         | Nk[u] UNION Nk[v] | 
#
#      Where Nk[u] is the k-neighborhood of C[u]. 
#
#  NOTE: The original specification inverts the numerator and the denominator. 
#        The size of the UNION will never be smaller than the size of the 
#        INTERSECTION. 
#
#   3. To compute the k-neighborhood of C[u], we compute a k-hop transitive 
#      closure over C[n x n]. We call this Nk[n x n]. NOTE: if k = 1, then 
#      there is no need to perform this step. Nk is C. 
#
#   4. To compute the I[n x n] (matrix of size of the INTERSECTION between
#      each pair u, v of n) we use spgemm ( Nk, transpose ( Nk ))
#
#   5. To compute U[n x n] we first compute the sum of calls in each u and v, 
#      and then subtract the size of the intersection. That is: 
#
#      | A U B | = | A | + | B | - | A n B | 
#
#   6. From the INTERSECTION and the UNION, we compute the Jaccard Distance. 
#      In fact, as: 
#
#            | A n B | 
#     JD =  ------------
#            | A U B | 
#
#      and | A U B | = | A | + | B | - | A n B |,
#
#      we can compute JD directly as: 
#
#                    | A n B | 
#     JD =  ---------------------------
#            | A | + | B | + | A n B | 
#
#
###############################################################################
#
#  Utility Functions: 
# set -x 
#
ulimit -a
ulimit -aH

onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

source /global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/Utils.sh
#
###############################################################################
#
#  Utility functions for Q3.sh 
#
#  Usage function for Q3.sh 
usage() { 
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "" 0;
  v_echo "\t${0} INPUT_ARRAY_NAME OUTPUT_ARRAY_NAME" 0;
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
};
#
#------------------------------------------------------------------------------
#
#  Functions and variables used exclusively in Q3.sh 
ADDG_ARRAY_NAME="CALLS";
OUTPUT_ARRAY_NAME="CALL_INTERSECT";
#
#  Dump the values of global variables for debugging purposes. 
dump_variable_status() { 
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "ADDG_ARRAY_NAME = ${ADDG_ARRAY_NAME}" 0;
  v_echo "OUTPUT_ARRAY_NAME    = ${OUTPUT_ARRAY_NAME}" 0;
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "" 0;
};
#
###############################################################################
#
#   Work Begins:
#
#  Check argument list has three elements. 
if [ "$#" -lt 2 ]; then
  error_usage_exit ${0} 1 3;
fi

if [ "$#" -gt 2 ]; then
  error_usage_exit ${0} 2 3;
fi
#
#
#------------------------------------------------------------------------------
#
# Adjacency DiGraph Array Name 
ADDG_ARRAY_NAME=$1
#
#
#  Check that named adjacency matrix exists. 
AC=$(check_array_exists ${ADDG_ARRAY_NAME});
if [ "${AC}" -ne 1 ]; then
  error_usage_exit ${0} 3 ${ADDG_ARRAY_NAME};
fi

#
#  Check that the named matrix has 2 dimensions. 
DC=$(get_adjacency_graph_dimension_count ${ADDG_ARRAY_NAME});
if [ "${DC}" -ne 2 ]; then
  error_usage_exit ${0} 4 ${ADDG_ARRAY_NAME} ${DC};
fi

#
#  Check that the named matrix has float attribute named "distance"
ATC=$(get_array_attributes_matching ${ADDG_ARRAY_NAME} "distance");
if [ "${ATC}" -ne 1 ]; then
  error_usage_exit ${0} 5 ${ADDG_ARRAY_NAME} "distance";
fi

#
#  Get the dimension details for the names array. 
populate_dim_details_for_array ${ADDG_ARRAY_NAME};

#
#  Check that the dimensions are consistent. 
if [ "${ADDG_ARRAY_DIMS_MAXS[0]}" -ne "${ADDG_ARRAY_DIMS_MAXS[0]}" ]; then 
  error_usage_exit ${0} 6 ${ADDG_ARRAY_NAME} "${ADDG_ARRAY_DIMS_MAXS[0]}" "${ADDG_ARRAY_DIMS_MAXS[0]}";
fi
#
#------------------------------------------------------------------------------
#
# Result Array Name 
OUTPUT_ARRAY_NAME=$2
#
#  OUTPUT_ARRAY_NAME=JD
TEMP_ARRAY_NAME="${OUTPUT_ARRAY_NAME}_TEMP"
INTERSECTION_ARRAY="${ADDG_ARRAY_NAME}_INTERSECTION"
STATE_NAME="${TEMP_ARRAY_NAME}";
#
RC=$(check_array_exists ${OUTPUT_ARRAY_NAME});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Result Array ${OUTPUT_ARRAY_NAME} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${OUTPUT_ARRAY_NAME}" > /dev/null;
fi

RC=$(check_array_exists ${TEMP_ARRAY_NAME});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Temp Array ${TEMP_ARRAY_NAME} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${TEMP_ARRAY_NAME}" > /dev/null;
fi

RC=$(check_array_exists ${INTERSECTION_ARRAY});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Temp Array ${INTERSECTION_ARRAY} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${INTERSECTION_ARRAY}" > /dev/null;
fi
#
################################################################################
# 
echo "BEGINNING";
date; 
#
CHUNK_LENGTH=4096
# CHUNK_LENGTH=8192
# CHUNK_LENGTH=16384
#
STATE_SHAPE="${ADDG_ARRAY_DIMS_NAMES[0]}=0:${ADDG_ARRAY_DIMS_MAXS[0]},${CHUNK_LENGTH},0, ${ADDG_ARRAY_DIMS_NAMES[1]}=0:${ADDG_ARRAY_DIMS_MAXS[1]},${CHUNK_LENGTH},0"
STATE_SCHEMA="< distance : float > [ ${STATE_SHAPE} ]"
#
INITIALIZE="
SELECT distance
  INTO ${STATE_NAME}
  FROM substitute ( 
        ( SELECT MIN( ditance ) AS distance
            FROM ${ADDG_ARRAY_NAME} 
          REDIMENSION BY [ ${STATE_SHAPE} ]
        ),
        build ( < distance : float > [ N=0:0,1,0 ], '[(0)]', true )
      )
"
#
INITIALIZE="
SELECT distance
  INTO ${STATE_NAME}
  FROM ${ADDG_ARRAY_NAME}
"
#
echo "INITIALIZE";
exec_aql_query_wd "${INITIALIZE}" -n 
#
CMD_COMPLETE_COMPUTE_JD="
SELECT COUNT(*) 
  FROM ( SELECT double ( I.INTER ) / double ( PL.PLUS - I.INTER ) AS JACCARD
           FROM ( SELECT multiply AS INTER
                    FROM spgemm ( ${STATE_NAME} AS R, 
                                  transpose(${STATE_NAME}) AS C )
                ) AS I,
                ( SELECT A.CNT + B.CNT AS PLUS
                    FROM ( SELECT COUNT(*) AS CNT
                             FROM ${STATE_NAME}
                           GROUP BY ${STATE_NAME}.${ADDG_ARRAY_DIMS_NAMES[0]}
                         ) AS A CROSS JOIN
                         ( SELECT COUNT(*) AS CNT
                             FROM ${STATE_NAME}
                           GROUP BY ${STATE_NAME}.${ADDG_ARRAY_DIMS_NAMES[0]} 
                         ) AS B
               ) AS PL
       ) WHERE JACCARD > 0.9

"
exec_aql_query_wd "${CMD_COMPLETE_COMPUTE_JD}"
#
date; 
echo "ENDING";
# 
echo "CHECKS ...";
#
exec_afl_query_wd "array_chunk_details ('CALLS')"
exec_afl_query_wd "array_chunk_details ('${STATE_NAME}')"
exec_aql_query_wd "SELECT COUNT(*) FROM ${STATE_NAME}"
#
#
onhead_stopscidb.sh