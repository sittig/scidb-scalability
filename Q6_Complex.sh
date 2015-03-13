#!/bin/bash
#
#   File:   Q6_Complex.sh
#
#  About: 
#
#------------------------------------------------------------------------------
#
#  Q6: PCA
#
#   Find the k best sets (k<d, say k=10 principal components) of most 
#   correlated dimensions with bad connections. Solved with SVD on the 
#   correlation matrix of the data set.
#
#------------------------------------------------------------------------------
#
ulimit -a
ulimit -aH

onhead_stopscidb.sh
onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

source /global/project/projectdirs/paralleldb/scidb_at_nersc/benchmark/graph/ksb_scripts/Utils.sh
#
HYGIENE="remove ( PCA_CRV_DATA )"
exec_afl_query_wd "${HYGIENE}"
exec_afl_query_wd "load_library('dense_linear_algebra')"
#
#   Get the basic values for the dimensions .... 
#
Q6_QUERY="
store ( 
  project (
    apply (
      gesvd (
        gemm (   
          project ( apply ( CRV_DATA, val, double ( value ) ), val ),
          project ( apply ( CRV_DATA, val, double ( value ) ), val ),
          build ( < val : double >
                  [ row_num=0:999,1000,0, 
                    row_num_2=0:999,1000,0 ],
                  0.0
                ),
          'TRANSA=1'
        ),
        'values'
      ),   
      result,   
      pow ( sigma, 0.5 )
    ),
    result
  ),
  PCA_CRV_DATA
)
"
echo "${Q6_QUERY}"
exec_afl_query_wd "${Q6_QUERY}" -n 
#
#------------------------------------------------------------------------------
onhead_stopscidb.sh