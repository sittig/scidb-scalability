#!/bin/bash
# 
#   File:   Benchmark/Graph/schema/Q2.sh 
#
#  About: 
#
#    The specification of Q2 taken from the benchmark is: 
#
#   "Q2: Given two nodes u and v, check if v is reachable from u. Given a 
#    node u in the graph, find the distance from it to all the nodes in the 
#    graph. This can be solved as a graph transitive closure computation."
#
#   NOTE: Q2 is essentially the same as Q2, except with the polarity 
#         reversed. That is, treat each in as an out. 
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
#  Utility functions for Q2.sh 
#
#  Usage function for Q2.sh 
usage() { 
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "" 0;
  v_echo "\t${0} INPUT_ARRAY_NAME src_vertex_id dst_vertex_id" 0;
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
};
#
#------------------------------------------------------------------------------
#
#  Functions and variables used exclusively in Q2.sh 
ADDG_ARRAY_NAME="CALLS";
IDX_START_VERT="10";
IDX_END_VERT="100";
#
#  TODO: Fix it so that I pull chunk the correct CHUNK_LENGTH
CHUNK_LENGTH=65536

#
#  Dump the values of global variables for debugging purposes. 
dump_variable_status() { 
  v_echo "" 0;
  v_echo "------------------------------------------------------------------------------" 0;
  v_echo "ADDG_ARRAY_NAME = ${ADDG_ARRAY_NAME}" 0;
  v_echo "IDX_START_VERT  = ${IDX_START_VERT}" 0;
  v_echo "IDX_END_VERT  = ${IDX_START_VERT}" 0;
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
# Initial vertex 
IDX_START_VERT=$2
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
#------------------------------------------------------------------------------
#
# End vertex 
IDX_END_VERT=$3
# 
#  IDX_END_VERT=100
#
#  Check that the IDX_END_VERT is within the dimension sizes. 
if [ "${IDX_END_VERT}" -lt "${ADDG_ARRAY_DIMS_MINS[0]}"\
     -o "${IDX_END_VERT}" -lt "${ADDG_ARRAY_DIMS_MINS[1]}"\
     -o "${IDX_END_VERT}" -gt "${ADDG_ARRAY_DIMS_MAXS[0]}"\
     -o "${IDX_END_VERT}" -gt "${ADDG_ARRAY_DIMS_MAXS[1]}" ]; then 

  error_usage_exit ${0} 7 ${ADDG_ARRAY_NAME} "${IDX_END_VERT}";
fi
#
#------------------------------------------------------------------------------
#
#   Check and setup the temp arrays used to hold the state. 
#
STATE_NAME="${STATE_NAME_NEXT}_PREV_";
STATE_NAME_NEXT="__TC_STATE__";
#
RC=$(check_array_exists ${STATE_NAME});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Result Array ${STATE_NAME} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${STATE_NAME}" > /dev/null;
fi
#
RC=$(check_array_exists ${STATE_NAME_NEXT});
if [ "${RC}" -eq "1" ]; then
  v_echo ">>>>> WARNING: Internal Array ${STATE_NAME_NEXT} exists - removing." 0;
  exec_aql_query "DROP ARRAY ${STATE_NAME_NEXT}" > /dev/null;
fi
#
# Check what phone numbers the index values correspond to ...
#
CMD_START_CHECK_PHONE_NUMS="
SELECT 'Index # ' + string ( ${IDX_START_VERT} ) + 
       ' is CALLING Phone # ' + string ( CALLING ) 
  FROM ( 
         SELECT MIN ( calling_phone ) AS CALLING
           FROM CALLS 
          WHERE calling_phone_ndx = ${IDX_START_VERT}
       )
"
exec_aql_query_wd "${CMD_START_CHECK_PHONE_NUMS}"
#
CMD_END_CHECK_PHONE_NUMS="
SELECT 'Index # ' + string ( ${IDX_END_VERT} ) + 
       ' is CALLED Phone # ' + string ( CALLED ) 
  FROM ( 
         SELECT MIN ( calling_phone ) AS CALLED
           FROM CALLS 
          WHERE calling_phone_ndx = ${IDX_END_VERT}
       )
"
exec_aql_query_wd "${CMD_END_CHECK_PHONE_NUMS}"
#
################################################################################
# 
#  Setup ... 
#
#  Load the target SciDB with the necessary libraries. . . 
# commented because I altered the Utils script to load this automatically -- ksb
# exec_afl_query "load_library('linear_algebra')" > /dev/null # for spgemm()
#
################################################################################
#
#  Begin Transitive Closure. 
#
#  Figure out the shapes of the input arrays.
STATE_SHAPE="${ADDG_ARRAY_DIMS_NAMES[1]}=0:${ADDG_ARRAY_DIMS_MAXS[1]},${ADDG_ARRAY_DIMS_CL[1]},0, dummy=0:0,1,0";
STATE_SCHEMA="< distance : float > [ ${STATE_SHAPE} ]"
#
PROJ_ARRAY_NAME="project ( ${ADDG_ARRAY_NAME}, distance )"
#
#  The general form of the iteration is: INIT, then ITER until CHECK = 1. 
#
#  INIT - create the STATE and the STATE_NEXT. 
INIT="
 SELECT D INTO ${STATE_NAME} FROM build ( < D : float > [ I=0:0,1,0 ], 0 );
 SELECT distance
   INTO ${STATE_NAME_NEXT} 
   FROM substitute ( 
          ( SELECT MIN(float(0)) AS distance 
              FROM build ( < ${ADDG_ARRAY_DIMS_NAMES[1]} : int64 > 
                           [ dummy=0:0,1,0 ], 
                           ${IDX_START_VERT} 
                         )
            REDIMENSION BY [ ${STATE_SHAPE} ]
          ),
          build ( <  V : float > [ MC=0:1,1,0 ], '[(0.0)]', true )
        )
"
#
#
# ITER - DROP the (previous) STATE, rename the NEXT to PREV, compute NEXT. 
ITER="
DROP ARRAY ${STATE_NAME};
RENAME ARRAY ${STATE_NAME_NEXT} TO ${STATE_NAME};
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
SELECT TRANS_CLOSURE_COMPLETE.R + PATH_EXISTS.R 
  FROM ( SELECT iif ( C_JOIN.C = C_MERGE.C, 1, 0 ) AS R
           FROM ( SELECT count(*) AS C
                    FROM ${STATE_NAME} AS I1, ${STATE_NAME_NEXT} AS I2
                  WHERE abs(I1.distance - I2.distance) < 0.001 
                ) AS C_JOIN,
                ( SELECT count(*) AS C 
                   FROM merge ( ${STATE_NAME}, ${STATE_NAME_NEXT} )
                ) AS C_MERGE
       ) AS TRANS_CLOSURE_COMPLETE,
       ( SELECT iif ( C > 0, 1, 0 ) AS R 
           FROM  ( SELECT count(*) AS C
                     FROM between ( ${STATE_NAME_NEXT},
                                    ${IDX_END_VERT}, null,
                                    ${IDX_END_VERT}, null )
                 )
       ) AS PATH_EXISTS
"
#
################################################################################
#
#  Compute TC
#
# echo "INIT"
# echo "${INIT}"
# echo "" 
# echo "ITER"
# echo "${ITER}"
# echo "" 
# echo "CHECK"
# echo "${CHECK}"
#
echo "--------------------------------------------+"
echo "| IDX_START_VERT (${IDX_START_VERT}) to IDX_END_VERT (${IDX_END_VERT} ) |"
echo "--------------------------------------------+"
#
time compute_iteration "${INIT}" "${ITER}" "${CHECK}" 10 
#
CMD_TEST_QUERY="
SELECT iif ( C > 0, 
             'Connected with distance = ' + string ( D ),
             'NOT Connected'
           ) 
  FROM  ( SELECT count(*) AS C, MIN ( distance ) AS D
            FROM between ( ${STATE_NAME_NEXT}, 
                           ${IDX_END_VERT}, null,
                           ${IDX_END_VERT}, null )
        )
"
#
exec_aql_query_wd "${CMD_TEST_QUERY}"
#
################################################################################
#
#  Do some sanity checks on the result.
#
# exec_aql_query_wd "SELECT * FROM show ( ${STATE_NAME} );"
#
#  {i} schema
#  {0} 'TC_TEMP_PREV<distance:float> 
#      [calling_phone=0:163417,65536,0,dummy=0:0,1,0]'
#
# exec_aql_query_wd "SELECT * FROM quantile(${STATE_NAME}, 5, distance);"
#
#  {quantile} percentage,distance_quantile
#  {0} 0,0ø
#  {1} 0.2,2
#  {2} 0.4,2
#  {3} 0.6,2
#  {4} 0.8,2
#  {5} 1,2
#
# exec_aql_query_wd "SELECT * FROM array_chunk_details('${STATE_NAME}');"
#
#  {i} num_chunks,num_cells,min_cells_per_chunk,max_cells_per_chunk,avg_cells_per_chunk
#  {0} 6,         110364,   5180,               28941,              18394
#
#
################################################################################
onhead_stopscidb.sh