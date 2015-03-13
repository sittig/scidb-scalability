#!/bin/bash
#
#   File:   Q5_Complex.sh
#
#  About: 
#
#------------------------------------------------------------------------------
#
#  Q5: Clustering
#
#   Clustering:  obtain k=10 groups of connections with similar characteristics
#   (5 dimensions based on previous analysis with good variance, low number of 
#    zero values, no missing information). 
#
#   Solved with K-means clustering, the most widely used clustering method.
#
#   So. I have the following input ... 
#
#              \  attribute
#   connections \       attr1   attr2   attr3   attr4   attr5 
#                \    +-------+-------+-------+-------+-------+
#       C1            |       |       |       |       |       |
#                     +-------+-------+-------+-------+-------+
#       C2            |       |       |       |       |       |
#                     +-------+-------+-------+-------+-------+
#      ...               ...     ...      ...    ...     ... 
#                     +-------+-------+-------+-------+-------+
#       Cn            |       |       |       |       |       |
#                     +-------+-------+-------+-------+-------+
#
#  NOTE:  There are a lot more than 5 attributes in the CRV_DATA set. There 
#         are actually about 109. But these 5 have been selected based on 
#         the fact that they have the most 'variance' in the data, meaning 
#         there's a lot of useful information in these attributes. 
#
#    1. Pick a random set of connections. These are the SEED values for 
#       the algorithm. For the purposes of this description let's let 
#       |SEED| = 3. 
#
#      seed
#   connections
#      C10
#      C20
#      C30 
#
#    2. For each seed, compute the average of the attributes for that 
#       connection. 
#
#       seed   \  average 
#   connections \            
#                \        +-------+
#       C10               | mean  | // mean of INPUT[C10, *]
#                         +-------+
#       C20               | mean  | // mean of INPUT[C20, *]
#                         +-------+
#       C30               | mean  | // mean of INPUT[C30, *]
#                         +-------+
#
#   3. For each seed, compute the sum of the differences sequared between 
#      the seed's mean value, and all of the attributes of the other 
#      connections. That is, you want to compute a matrix that looks like
#      this:
#
#      seed    \  connections
#   connections \        C1      C2     C3       C4              Cn     
#                \    +-------+-------+-------+-------+       +-------+
#       C10           | dist  | dist  | dist  | dist  |  ...  | dist  |
#                     +-------+-------+-------+-------+       +-------+
#       C20           | dist  | dist  | dist  | dist  |  ...  | dist  |
#                     +-------+-------+-------+-------+       +-------+
#       C30           | dist  | dist  | dist  | dist  |  ...  | dist  |
#                     +-------+-------+-------+-------+       +-------+
#
#    NOTE: 'dist' is the sum ( pow ( C1.avg - Cn.value ), 2 ) 
#
#  4. For each seed, find the Cn with the smallest dist. This is to become 
#     the connection that is the newest "center" of the cluster. 
#
#      seed    \  connections
#   connections \        C1    ...  C113   ...   C415  ...  C1076  ...   Cn
#                \    +-------+   +-------+   +-------+   +-------+   +------+
#       C10           |       |...| dist  |...|       |...|       |...|      |
#                     +-------+   +-------+   +-------+   +-------+   +------+
#       C20           |       |...|       |...|       |...| dist  |...|      |
#                     +-------+   +-------+   +-------+   +-------+   +------+
#       C30           |       |...|       |...| dist  |...|       |...|      |
#                     +-------+   +-------+   +-------+   +-------+   +------+
#
#  5. The list of |SEED| connections becomes the new list of seed centers, and 
#     we proceed to step 2, and step 2 
#
#       C113
#       C415 
#       C1076 
# 
#       seed   \  average 
#   connections \            
#                \       +-------+
#       C113             | mean  | // mean of INPUT[C1113, *]
#                        +-------+
#       C415             | mean  | // mean of INPUT[C415, *]
#                        +-------+
#       C1076            | mean  | // mean of INPUT[C1076, *]
#                        +-------+
#
#   6. Now. If ... 
# 
#      a ) there is no difference between the means of each cluster found in the 
#          previous iteration, or 
# 
#      b ) the number of iterations exceeds the threshhold, 
#
#      terminate the algorithm.
#
#      Otherwise, go back to step 3. 
#
#------------------------------------------------------------------------------
#
source ../../Utils/Utils.sh 
#
# CMD_LOAD_LINEAR_ALGEBRA_LIBRARY="load_library ( 'linear_algebra' )"
# exec_afl_query_wd "${CMD_LOAD_LINEAR_ALGEBRA_LIBRARY}"
#
#------------------------------------------------------------------------------
#
CMD_HYGIENE_GROUPS="DROP ARRAY GROUPS"
exec_aql_query_wd "${CMD_HYGIENE_GROUPS}"
CMD_HYGIENE_PREV_GROUPS="DROP ARRAY PREV_GROUPS"
exec_aql_query_wd "${CMD_HYGIENE_PREV_GROUPS}"
CMD_HYGIENE_CENTERS="DROP ARRAY CENTERS"
exec_aql_query_wd "${CMD_HYGIENE_CENTERS}"
CMD_HYGIENE_DIST="DROP ARRAY DIST"
exec_aql_query_wd "${CMD_HYGIENE_DIST}"
#
#------------------------------------------------------------------------------
#
#   I'm going to implement this as a general operation - k-means. 
#
#   k-means INPUT_ARRAY OUTPUT_ARRAY
#
#   The operation takes: 
#
#    1. An array 'A', with dimensions 'i' and 'j', and an attribute 'val'.
#    2. The number of centers you wish to construct in the cluster - 'k'
#    3. A limit on the number of iterations (in case it does not converge) - 'l'
#
#   The procedure is: 
#
#    1. Initialize: 
#       1.1 Pick 'k' values (at random) over the range o..|j| which are to 
#           serve as the seeds c in 'centers'. In this case, we're 
#
#    2. Iterate: 
#       2.1 Compute the 'mean' (avg) value for A[c].val for each c in 'centers' 
#       2.2 Compute, for each row in A[j], the sum of ( c.avg - A[i,J].val ) ^ 2
#           Call this 'dist'. 
#       2.3 
#
#  lloyd <- function(x, num.centers, iter.max=30)
#  {
#    if(length(dim(x))!=2) stop("x must be a matrix")
#    if(!is.scidb(x)) stop("x must be a scidb object")
#
#    x <- project(x, x@attributes[1])
#    x <- attribute_rename(x,new="val")
#    x <- dimension_rename(x,new=c("i","j")) 
#                            // which is connections? which attr?
#
#    expr <- sprintf("random() %% %d", num.centers)
#                            // WHERE THE FUCK did 'd' come from? 
#                            // OK. It's an argument to the expr. 
#    
#    group <- build(expr, nrow(x), names=c("group","i"), type="int64")
#                            // number of rows in x is the 'i'?
# 
#    for(iter in 1:iter.max)
#    {
#      centers <- aggregate(x, by=list(group, "j"), FUN=mean, eval=TRUE)
#       //
#       // Compute the average (mean) value of each of the rows/connections 
#       // in the 'group' over all of the attributes. 
#       // 
# 
#      dist <- aggregate(
#                bind(
#                  merge(x, centers, by="j"),
#                  "dist", "(val - val_avg)*(val - val_avg)"
#                ),
#                by=list("i","group"),
#                FUN="sum(dist) as dist", unpack=FALSE, eval=TRUE
#              )
#        //
#        //  Compute the sum of the differences between the per-center 
#        // 'average' and all of the attribute values for each row. 
#        // 
# 
#      oldgroup <- group
#        // 
#        //  Assign group -> oldgroup.
#        // 
#     
#      group <- redimension(
#                 Filter("dist = min",
#                   merge(dist,
#                     aggregate(dist,by="i", 
#                               FUN="min(dist) as min", 
#                               unpack=FALSE),
#                       by="i")
#                 ),group, eval=TRUE)
#        // 
#        // Compute the new group. These are the rows  with the sum of the 
#        // differences between the attribute's squared is minimized. 
# 
#      if(sum(abs(oldgroup - group)) < 1) break
#        //
#        // check the difference between the previous groups, and the last
#        // one computed. If the difference is small, break. Otherwise, 
#        // loop again. 
#        // 
#     
#    }
#    if(iter==iter.max) waring("Reached maximum # iterations")
#    list(cluster = group,
#         centers = centers)
#  }
#  
#  # A simple little example follows.
#  # Let's generate data with obvious clusters.
#  set.seed(1)
#  x=c(x=rnorm(50),rnorm(50)+3);y = c(rnorm(50),rnorm(50)+3)
#  A = cbind(x,y)
#  # Plot the data (try to set the aspect ratio square)
#  # Let a be an m x n matrix.
#  # Each row of the matrix A is a point in an n-dimensional Euclidean space.
#  library("scidb")
#  scidbconnect("127.0.0.1")   # Replace with the ipaddress of your SciDB coord.
#  A = as.scidb(A)
#  k = lloyd(A, 2, 10)
#  plot(A[], asp=1,xlab="x",ylab="y")
#  points(A[k$cluster %==%0, ][], pch=19,col=4)
#  
#------------------------------------------------------------------------------
#
#   1. The array 'A' will be the CRV_DATA. Each 'row_num' corresponds to 
#      a connection. 
#
#   2. The list of attributes (attr_num) entries are the non-identifying 
#      ones (we exclude 'i') which have the highest variance (stdev). These
#      are:
#
ATTRIBUTES_LIST="redimension ( 
  build (
    < attr_num : int64, v : int32 > [ R=0:*,10,0 ],
    '[(7,1),(75,1),(41,1),(8,1),(76,1)]',
    true
  ),
  < v : int32 > [ attr_num=0:*,1000,0 ]
)"
#
# Test: 
# exec_afl_query_wd "${ATTRIBUTES_LIST}"
#
#
ATTRIBUTES_LIST_AQL="
SELECT MIN ( dv ) AS v
  FROM build ( 
         < attr_num : int64, dv : int32 > [ R=0:*,10,0 ],
         '[(7,1),(75,1),(41,1),(8,1),(76,1)]',
         true
       )
 REDIMENSION BY [ attr_num=0:*,1000,0 ]
"
# exec_aql_query_wd "${ATTRIBUTES_LIST_AQL}"
# 
#  {i} count
#  {0} 5
#  Elapsed Time: 0:00.02
#
#   3. So. Which attribute's data is relevant? 
#
#  NOTE: In a cross_join, the *second* argument is replicated. 
#
ATTRIBUTE_DATA="
cross_join ( 
  CRV_DATA,
  ${ATTRIBUTES_LIST} AS ATTR_LIST,
  CRV_DATA.attr_num, ATTR_LIST.attr_num
)
"
# exec_afl_query_wd "aggregate ( ${ATTRIBUTE_DATA}, count(*) )"
#
#  {i} count
#  {0} 5000000
# Elapsed Time: 0:04.52
#
ATTRIBUTE_DATA_AQL="
SELECT value
  FROM CRV_DATA CROSS JOIN ( ${ATTRIBUTES_LIST_AQL} ) AS ATTR_LIST 
 WHERE CRV_DATA.attr_num = ATTR_LIST.attr_num
"
# exec_aql_query_wd "${ATTRIBUTE_DATA_AQL}"
#
#------------------------------------------------------------------------------
#  
#   3. Let's build our initial list CENTER. This will have 10 seeds. 
#
SEEDS=10
SEED_MAX=$((SEEDS - 1));
#
#   3.1 Figure out the shape of the array to hold the seeds. The seeds are 
#       10 "connections" (read row_num) choosen at random from the 
#       entire set of connections. 
#
QUERY_LENGTH_OF_DIMENSION=" SELECT high - low FROM dimensions(CRV_DATA) WHERE name = 'row_num'"
DIM_CNT=$(get_single_value "${QUERY_LENGTH_OF_DIMENSION}");
DIM_LEN=$((DIM_CNT + 1));
#
CHUNK_LEN=$(get_single_value "SELECT chunk_interval FROM dimensions(CRV_DATA) WHERE name = 'row_num'");
#
#   Print out what you found 
v_echo "DIM_CNT (connection count)  = ${DIM_CNT}, s0 DIM_LEN=${DIM_LEN} AND CHUNK_LEN = ${CHUNK_LEN}" 0 
#
#   What is the shape of the "vector" to be used to hold the CENTERS?
DIM_MAX=$(( $DIM_CNT - ( $DIM_CNT % $CHUNK_LEN ) + $CHUNK_LEN - 1));

echo "DIM_MAX = ${DIM_MAX}";

CENTER_SHAPE="row_num=0:${DIM_MAX},${CHUNK_LEN},0"
v_echo "GROUP SHAPE= ${CENTER_SHAPE}" 0
#
CMD_BUILD_RANDOM_INITIAL_CENTERS="
redimension ( 
  apply (
    build ( < row_num : int64 > [ SEED=0:${SEED_MAX},${SEEDS},0 ], 
            random()%${DIM_LEN}
          ),
    V, 1 
  ),
  < V : int64 NULL > [ ${CENTER_SHAPE} ],
  MIN(V) AS V
)
"
# exec_afl_query_wd "${CMD_BUILD_INITIAL_CENTERS}"
#
BUILD_RANDOM_INITIAL_CENTERS_AQL="
SELECT MIN ( 1 ) AS V 
  FROM build ( < row_num : int64 > [ SEED=0:${SEED_MAX},${SEEDS},0 ],
               random()%${DIM_LEN}
             )
REDIMENSION BY [ ${CENTER_SHAPE} ]
"
# exec_aql_query_wd "${BUILD_INITIAL_CENTERS_AQL}"
#
#------------------------------------------------------------------------------
#
CMD_BUILD_INITIAL_CENTERS="
redimension (
  apply (
    build ( < row_num : int64 > [ SEED=0:${SEED_MAX},${SEEDS},0 ],
            (${DIM_LEN}-1)/(SEED+1)
          ),
    V, 1
  ),
  < V : int64 NULL > [ ${CENTER_SHAPE} ],
  MIN(V) AS V
)
"
# exec_afl_query_wd "${CMD_BUILD_INITIAL_CENTERS}"
#
BUILD_INITIAL_CENTERS_AQL="
SELECT MIN ( 1 ) AS V
  FROM build ( < row_num : int64 > [ SEED=0:${SEED_MAX},${SEEDS},0 ],
               (${DIM_LEN}-1)/(SEED+1)
             )
REDIMENSION BY [ ${CENTER_SHAPE} ]
"
exec_aql_query_wd "${BUILD_INITIAL_CENTERS_AQL}"
#
#  For the purposes of testing, we will set the initial starting points to 
#  ensure that get consistent answers, rather than to 
#
#------------------------------------------------------------------------------
#
COMPUTE_PER_CENTER_AVERAGES="
aggregate ( 
  cross_join ( 
    ${ATTRIBUTE_DATA} AS DATA,
    ${CMD_BUILD_INITIAL_CENTERS} AS CENTERS,
    DATA.row_num, CENTERS.row_num 
  ),
  avg ( value ) AS avg_val,
  row_num
)
"
# exec_afl_query_wd "${COMPUTE_PER_CENTER_AVERAGES}"
#
COMPUTE_PER_CENTER_AVERAGES_AQL="
SELECT AVG ( value ) AS avg_val
  FROM ( ${ATTRIBUTE_DATA_AQL} ) AS DATA CROSS JOIN 
       ( ${BUILD_INITIAL_CENTERS_AQL} ) AS CENTERS 
 WHERE DATA.row_num = CENTERS.row_num
GROUP BY DATA.row_num
"
# exec_aql_query_wd "${COMPUTE_PER_CENTER_AVERAGES_AQL}"
#
#  Wolvi: Elapsed Time: 8:26.54
#
#  NOTE: It does not look as if the Translator.cpp does "the right thing" 
#        when re-writing A CROSS JOIN B WHERE A.i = B.i into the 
#        correct cross ( A, B, A.i, B.i ). Instead, it turns into 
#        filter ( cross ( A, B ), A.i = B.i ), which is much less efficient. 
#
CMD_COMPUTE_AND_STORE_PER_CENTER_AVERAGES="
store ( 
  ${COMPUTE_PER_CENTER_AVERAGES},
  GROUPS
)"
# exec_afl_query_wd "${CMD_COMPUTE_AND_STORE_PER_CENTER_AVERAGES}"
#
#
#   ----- INIT ----- 
INIT="
store ( 
  ${COMPUTE_PER_CENTER_AVERAGES},
  GROUPS
)"
#
# Elapsed Time: 0:01.72
#
#------------------------------------------------------------------------------
#
#   To figure out which values to use to compute the means of each center 
#  across each of the selected attributes, we'll construct a "mask" by 
#  cross_join of the attributes (those with the highest stdev which were not 
#  identifiers) and the row_numbers. 
#    
#   Compute the average value of the attributes in the CRV_DATA for each of 
#  the randomly selected centroids. 
#
CALC_DIST="
aggregate (
  apply (
    cross_join ( ${ATTRIBUTE_DATA} AS DATA, PREV_GROUPS AS G ),
    pair_dist , pow (( G.avg_val - DATA.value ) , 2.0 )
  ),
  sum ( pair_dist ) as total_dist,
  G.row_num, DATA.row_num
)
"
# exec_afl_query_wd "${CALC_DIST}" -n 
#
CMD_CALC_AND_STORE_DIST="
CREATE TEMP ARRAY DIST
< total_dist:double NULL DEFAULT null >
[row_num=0:*,1000,0,row_num_2=0:*,1000,0];
store ( 
  ${CALC_DIST},
  DIST
)
"
# exec_afl_query_wd "show ('${CMD_CALC_AND_STORE_DIST}', 'afl')"
# exec_afl_query_wd "${CMD_CALC_AND_STORE_DIST}" -n 
#
#   Wolvi: 2:14, 4:43  saving 2x the data 
#
#   Which other row (which other connection) is closest to the center of 
#  the cluster? 
#
CALC_NEW_GROUPS="
aggregate ( 
  cross_join ( 
    ${ATTRIBUTE_DATA} AS DATA,
    redimension (
      cast (
        substitute (
          aggregate (
            filter (
              cross_join (
                apply ( DIST, rn2, row_num_2 ) AS ALL_DIST,
                aggregate (
                  DIST,
                  min ( total_dist ) AS min_dist,
                  row_num
                ) AS MIN_DIST_PER_ROW,
                ALL_DIST.row_num, MIN_DIST_PER_ROW.row_num
              ),
              ALL_DIST.total_dist = MIN_DIST_PER_ROW.min_dist
            ),
            min ( rn2 ),
            row_num
          ),
          build ( < v : int64 > [ I=0:0,1,0 ], '[(0)]', true )
        ),
        < row_num : int64  > [ old_center=0:*,1000,0 ]
      ),
      < old_center : int64 > [ row_num=0:*,1000,0 ]
    ) AS NEW_CENTER,
    DATA.row_num, NEW_CENTER.row_num
  ),
  avg ( value ) AS avg_val,
  row_num
)
"
# exec_afl_query_wd "${CALC_NEW_GROUPS}"
#
CMD_RENAME_GROUPS="rename ( GROUPS, PREV_GROUPS )"
# exec_afl_query_wd "${CMD_RENAME_GROUPS}"
#
CMD_CALC_AND_STORE_NEW_GROUPS="
store ( 
  ${CALC_NEW_GROUPS},
  GROUPS
)
"
# exec_afl_query_wd "show ('${CMD_CALC_AND_STORE_NEW_GROUPS}', 'afl')"
# exec_afl_query_wd "${CMD_CALC_AND_STORE_NEW_GROUPS}" 
#
#  ----- ITER -----
#
ITER="
${CMD_RENAME_GROUPS};
${CMD_CALC_AND_STORE_DIST};
store ( 
  ${CALC_NEW_GROUPS},
  GROUPS
);
remove ( DIST );
"
#
#-------------------------------------------------------------------------------
# 
#  NOTE: For the definition of this macro, please consult the macros.txt file 
#        in ../../Utils/macros.txt. These macros are loaded into SciDB by 
#        the Utils.sh script, invoked in the Create_and_Load.sh.
#
CHECK_SAME_ARRAY="
identicalReal (
  GROUPS,
  PREV_GROUPS, 
  GROUPS.avg_val,
  PREV_GROUPS.avg_val
)"
# exec_afl_query_wd "${CHECK_SAME_ARRAY}"
#
CMD_DROP_PREV_GROUP="remove ( PREV_GROUPS )"
# exec_afl_query_wd "${CMD_DROP_PREV_GROUP}"
#
#  ---- CHECK ---- 
#
CHECK="
project ( 
  apply ( 
    ${CHECK_SAME_ARRAY},
     check,
     iif ( res, 1, 0 ) 
  ),
  check 
);
remove ( PREV_GROUPS );
"
#
#-------------------------------------------------------------------------------
#
#  SETUP 
CMD_HYGIENE_GROUPS="DROP ARRAY GROUPS"
exec_aql_query_wd "${CMD_HYGIENE_GROUPS}"
CMD_HYGIENE_PREV_GROUPS="DROP ARRAY PREV_GROUPS"
exec_aql_query_wd "${CMD_HYGIENE_PREV_GROUPS}"
CMD_HYGIENE_CENTERS="DROP ARRAY CENTERS"
exec_aql_query_wd "${CMD_HYGIENE_CENTERS}"
CMD_HYGIENE_DIST="DROP ARRAY DIST"
exec_aql_query_wd "${CMD_HYGIENE_DIST}"
#
# exec_afl_query_wd "${INIT}"
# exec_afl_query_wd "${ITER}" -n 
# exec_afl_query_wd "${CHECK}"
# exec_aql_query_wd "SELECT * FROM GROUPS"
# exec_aql_query_wd "SELECT * FROM PREV_GROUPS"
#
#-------------------------------------------------------------------------------
#
echo "INIT = ${INIT}"
echo "ITER = ${ITER}"
echo "CHECK = ${CHECK}"
#
date;
time f_compute_iteration "${INIT}" "${ITER}" "${CHECK}" 10 
#
exec_aql_query_wd "SELECT * FROM GROUPS;"
#
exit;
#
#-------------------------------------------------------------------------------
