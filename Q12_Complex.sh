#!/bin/bash
#
#   File:   Q12_Complex.sh
#
#  About: 
#
#------------------------------------------------------------------------------
#
#   Q1: Understand the probabilistic distribution of all numeric attributes. 
#       Compute the mean, standard deviation all numeric attributes.
#
#   Q2: Understand the probabilistic distribution of all numeric attributes. 
#       Compute the median (values at 50% percentile).
#
#------------------------------------------------------------------------------
#

ulimit -a
ulimit -aH

onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

source /global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/Utils.sh

#
#  Q0: Sanity 
#
echo "+----------------+"
echo "|  Q1 COUNT(*)   |"
echo "+----------------+"
CMD_COMPUTE_COUNT_ALL="
SELECT COUNT(*) FROM CRV_DATA
"
time exec_aql_query_wd "${CMD_COMPUTE_COUNT_ALL}"
#
#  Q1:
echo "+--------------------+"
echo "|  Q1 - AVG, STDEV   |"
echo "+--------------------+"
CMD_HYGIENE_M_STD_ALL="DROP ARRAY CRV_MEAN_STD"
time exec_aql_query_wd "${CMD_HYGIENE_M_STD_ALL}" -n 
#
CMD_COMPUTE_M_STD_ALL="
SET NO FETCH;
SELECT AVG( value ) AS AVG_VAL,
       STDEV ( value ) AS STD_VAL
  INTO CRV_MEAN_STD
  FROM CRV_DATA
GROUP BY attr_num
"
time exec_aql_query_wd "${CMD_COMPUTE_M_STD_ALL}" 
#
# Result check: which three attributes have the largest stdev and what is the 
#               mean value in each case? 
#
CMD_COMPUTE_MED_ALL="
SET NO FETCH;
SELECT MEDIAN( value ) AS MED_VAL
  FROM CRV_DATA
GROUP BY attr_num
"
# ksb -- implemented Mar.11
#exec_afl_query_wd "load_library('linear_algebra')"
#CMD_COMPUTE_MED_ALL="
#aggregate(CRV_DATA,median( value ))
#"
#exec_afl_query_wd "${CMD_COMPUTE_MED_ALL}"
#
CMD_MEAN_STD_CHECK_RESULT="
SELECT ATTR_NUM, AVG_VAL, STD_VAL
  FROM sort ( CRV_MEAN_STD, STD_VAL DESC ) 
 WHERE ATTR_NUM > 1 AND ATTR_NUM < 36
"
# exec_aql_query_wd "${CMD_MEAN_STD_CHECK_RESULT}" 
#
#
echo "+--------------------------------+"
echo "|  Q1.1 - 5 with highest STDEV   |"
echo "+--------------------------------+"
CMD_COMPUTE_TOP_N_STDEVS="
SELECT * 
  FROM sort ( 
        ( SELECT S.STD_VAL * N.NUM_NON_ZEROES AS METRIC,
                 S.attr_num AS ATTR_NUM
            FROM 
            ( SELECT STDEV ( value ) AS STD_VAL
                FROM CRV_DATA
              GROUP BY attr_num
            ) AS S, 
            ( SELECT COUNT ( * ) AS NUM_NON_ZEROES
                FROM CRV_DATA
              WHERE value > 0
              GROUP BY attr_num
            ) AS N
          ) , 
          METRIC DESC
       )
 WHERE n < 6
"

# ksb -- uncommented next line. comment again if necessary
time exec_aql_query_wd "${CMD_COMPUTE_TOP_N_STDEVS}"
#
#  {n} METRIC,      ATTR_NUM
#  {0} 1.46179e+12, 7
#  {1} 1.46179e+12, 41
#  {2} 1.46179e+12, 75
#  {3} 2.88675e+11, 0
#  {4} 4.25484e+09, 8
#  {5} 4.25484e+09, 42
#  Elapsed Time: 0:13.39
#
#   NOTE: Why 6? Well ... attr_num 0 is 'i', which is an identifier. 
#        
#  Q2 
echo "+--------------------+"
echo "| Q2 - MEDIAN        |"
echo "+--------------------+"
CMD_COMPUTE_MEDIAN_ALL="
SELECT MEDIAN( value ) AS MEDVAL
  FROM CRV_DATA
GROUP BY attr_num
"
#
#  NOTE: This query takes way too long. 
# ksb note to self: maybe uncomment later for fun?
# exec_aql_query_wd "${CMD_COMPUTE_MEDIAN_ALL}" -n 
#
echo "+------------------------+"
echo "| Q2.1 - MEDIAN  as SORT |"
echo "+------------------------+"
#
#  NOTE: By sorting the entire data set by first, attribute number, and 
#        then value-within-attribute-number, we arrive at a sequence 
#        where the "median" will be the "middle" (length / 2) value 
#        in the sequence. 
#
ROW_DIM_QUERY="SELECT high FROM dimensions ( CRV_DATA ) WHERE name = 'row_num'"
ROW_DIM_LENGTH=$(get_single_value "${ROW_DIM_QUERY}");
CMD_COMPUTE_MEDIAN_AS_SORT="
filter ( 
  sort ( 
    apply ( CRV_DATA, 
            attribute, 
            attr_num 
          ),
    attribute, value
  ),
  n%${ROW_DIM_LENGTH} = ( ${ROW_DIM_LENGTH} / 2 )
)
"
time exec_afl_query_wd "${CMD_COMPUTE_MEDIAN_AS_SORT}" 
#
#  AND attribute > 1 
#  AND attribute < 37
#
#------------------------------------------------------------------------------


onhead_stopscidb.sh