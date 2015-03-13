#!/bin/bash
# 
#   File:   Benchmark/Graph/schema/Q1.sh 
#
#  About: 
#
#    The specification of Q1 from the benchmark is: 
#
#  " Q1: Given a node u in the graph, find the distance from it to all the 
#    nodes in the graph. Distance will be given by the number of edges in 
#    the path. This is a generalization of shortest path problem with 
#    one source node to multiple source nodes. "
#
#    This script implements a Bellman-Ford algorithm that computes the 
#   single-source / all-destinations shortest path distance result. This 
#   script is based on the example in the P4 trunk. 
#
#    This program takes as input a (square, sparse) matrix of size n x n that 
#   corresponds to an adjacency matrix for a set of 'n' vertices. The input 
#   matrix includes (at least) a floating point attribute named 'distance' that
#   reflects the 'distance' or 'weight' between the i and j vertices of the 
#   directed graph. That is: 
#
#   CREATE ARRAY CALL_GRAPH 
#   < 
#      ...
#      distance : float,
#      ...
#   > 
#   [ I=0:*,?,0, J=0:*,?,0 ];
#
#    Note that the actual lengths of the CALL_GRAPH.I and CALL_GRAPH.J must 
#   be the same. 
#
###############################################################################
#
#  Utility Functions: 
#

ulimit -a
ulimit -aH

onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

source /global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/Utils.sh
#
###############################################################################
#
#  Utility functions for Q1.sh 
#
#  Usage function for Q1.sh 
usage() { 
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "" 0;
  v_echo "\t${0} INPUT_ARRAY_NAME OUTPUT_ARRAY_NAME src_vertex_id" 0;
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
};
#
#------------------------------------------------------------------------------
#
#  Functions and variables used exclusively in Q1.sh 
ADDG_ARRAY_NAME="CALLS";
R_ARRAY_NAME="PL_CALLS";
IDX_START_VERT="10";
#
#  TODO: Fix it so that I pull chunk the correct CHUNK_LENGTH
# ksb -- changed to reflect different chunk length. change back if necessary!
#CHUNK_LENGTH=65536 ???????
CHUNK_LENGTH=30431
#
#  Dump the values of global variables for debugging purposes. 
dump_variable_status() { 
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "ADDG_ARRAY_NAME = ${ADDG_ARRAY_NAME}" 0;
  v_echo "R_ARRAY_NAME    = ${R_ARRAY_NAME}" 0;
  v_echo "IDX_START_VERT  = ${IDX_START_VERT}" 0;
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "" 0;
};
#
###############################################################################
#
#   Work Begins:
#
#  Check argument list has three elements. 
if [ "$#" -lt 3 ]; then
  error_usage_exit ${0} 1 3;
fi

if [ "$#" -gt 3 ]; then
  error_usage_exit ${0} 2 3;
fi
#
#------------------------------------------------------------------------------
#
# Adjacency DiGraph Array Name 
ADDG_ARRAY_NAME=$1
#
#  ADDG_ARRAY_NAME="CALLS"

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
if [ "${ADDG_ARRAY_DIMS_MAXS[0]}" -ne "${ADDG_ARRAY_DIMS_MAXS[1]}" ]; then 
  error_usage_exit ${0} 6 ${ADDG_ARRAY_NAME} "${ADDG_ARRAY_DIMS_MAXS[0]}" "${ADDG_ARRAY_DIMS_MAXS[1]}";
fi
#
#------------------------------------------------------------------------------
#
# Result Array Name 
R_ARRAY_NAME=$2
#  R_ARRAY_NAME=SP_CALLS
#
#  NOTE: This is a bit subtle. I want the name of the array that holds the 
#        output state to be the name provided by the user. So I need to align 
#        the desired output (R_ARRAY_NAME) with the name of the array/vector 
#        holding the state at the CHECK of each iteration (which is 
#        STATE_NAME_NEXT. 
#
STATE_NAME_NEXT="${R_ARRAY_NAME}";
STATE_NAME="${R_ARRAY_NAME}_PREV";
#
RC=$(check_array_exists ${STATE_NAME});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Result Array ${STATE_NAME} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${STATE_NAME}" > /dev/null;
fi

RC=$(check_array_exists ${STATE_NAME_NEXT});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Internal Array ${STATE_NAME_NEXT} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${STATE_NAME_NEXT}" > /dev/null;
fi
#
#------------------------------------------------------------------------------
#
# Initial vertex 
IDX_START_VERT=$3
# 
#  IDX_START_VERT=10
#
#  Check that the IDX_START_VERT is within the dimension sizes. 
if [ "${IDX_START_VERT}" -lt "${ADDG_ARRAY_DIMS_MINS[0]}"\
     -o "${IDX_START_VERT}" -lt "${ADDG_ARRAY_DIMS_MINS[1]}"\
     -o "${IDX_START_VERT}" -gt "${ADDG_ARRAY_DIMS_MAXS[0]}"\
     -o "${IDX_START_VERT}" -gt "${ADDG_ARRAY_DIMS_MAXS[1]}" ]; then 

  error_usage_exit ${0} 7 ${ADDG_ARRAY_NAME} "${IDX_START_VERT}";
fi
#
################################################################################
# 
#   The maximum iteration count is the number of vertices, which is the 
#  dimension MAX. 
#
#  Figure out the shapes of the input arrays.
STATE_SHAPE="${ADDG_ARRAY_DIMS_NAMES[1]}=0:${ADDG_ARRAY_DIMS_MAXS[1]},${ADDG_ARRAY_DIMS_CL[0]},0, dummy=0:0,1,0";
STATE_SCHEMA="< distance : float > [ ${STATE_SHAPE} ]"
#
PROJ_ARRAY_NAME="project ( ${ADDG_ARRAY_NAME}, distance )"
#
#  The general form of the iteration is: INIT, then ITER until CHECK = 1. 
#
#  INIT - create the STATE and the STATE_NEXT. 
echo "STATE_NAME = ${STATE_NAME};"
echo "STATE_NAME_NEXT = ${STATE_NAME_NEXT};"
#
INIT="
 SELECT * INTO ${STATE_NAME} FROM build ( < D : float > [ I=0:0,1,0 ], 0 );
 SELECT * 
   INTO ${STATE_NAME_NEXT} 
   FROM substitute ( 
          ( SELECT MIN(float(0)) AS distance 
              FROM build ( < calling_phone_ndx : int64 > [ dummy=0:0,1,0 ], 
                           ${IDX_START_VERT} 
                         )
            REDIMENSION BY [ ${STATE_SHAPE} ]
          ),
          build ( <  V : float > [ MC=0:1,1,0 ], '[(0.0)]', true )
        )
"
#
# ITER - DROP the (previous) STATE, rename the NEXT to PREV, compute NEXT. 
ITER="
DROP ARRAY ${STATE_NAME};
RENAME ARRAY ${STATE_NAME_NEXT} TO ${STATE_NAME};
CREATE TEMP ARRAY ${STATE_NAME_NEXT}
${STATE_SCHEMA};
SELECT multiply AS distance
  INTO ${STATE_NAME_NEXT}
  FROM spgemm ( ${PROJ_ARRAY_NAME}, ${STATE_NAME}, 'min.+' )
"
#
#  CHECK - terminate the iteration when the NEXT is the same as the PREVIOUS. 
#
#  Two arrays are the same when ( a ) they have the same number of cells (
# which means the count(join) == count(merge)) and ( b ) the difference 
# between the values of the cells is all less than some epsilon. 
CHECK="
SELECT iif ( C_JOIN.C = C_MERGE.C, 1, 0 ) AS R
  FROM ( SELECT count(*) AS C
           FROM ${STATE_NAME} AS I1, ${STATE_NAME_NEXT} AS I2
          WHERE abs(I1.distance - I2.distance) < 0.001 
       ) AS C_JOIN,
       ( SELECT count(*) AS C 
           FROM merge ( ${STATE_NAME}, ${STATE_NAME_NEXT} )
       ) AS C_MERGE;
"
#
iquery -nq "${INIT};" 
iquery -nq "${ITER};"  
#
################################################################################
#
#  Compute Bellman-Ford SP vector
echo "INIT = ${INIT}"
echo "ITER = ${ITER}"
echo "CHECK = ${CHECK}"
time compute_iteration "${INIT}" "${ITER}" "${CHECK}" 10 
#
################################################################################
#
#  Result? 
CMD_OUTPUT="SELECT MAX ( distance ) AS Radius  FROM ${R_ARRAY_NAME}"
exec_aql_query_wd "${CMD_OUTPUT}" 
#
################################################################################
#
#  Do some sanity checks on the result. 
#
# exec_aql_query_wd "SELECT * FROM show ( ${R_ARRAY_NAME} );"
#
# exec_aql_query_wd "SELECT MAX(distance) FROM ${R_ARRAY_NAME};"
# exec_aql_query_wd "SELECT * FROM quantile( ${R_ARRAY_NAME}, 5, distance);"
#
# exec_aql_query_wd "SELECT * FROM array_chunk_details('${R_ARRAY_NAME}');"
#
#  {i} num_chunks,num_cells,min_cells_per_chunk,max_cells_per_chunk,avg_cells_per_chunk
#  {0} 6,         165130,   9795,               40534,              27521.7
#
################################################################################
onhead_stopscidb.sh