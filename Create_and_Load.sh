#!/bin/bash
#
#   File :   Complex/schema/Create_and_Load.sh 
#
#  About : 
#
#    This file contains the AFL/AQL to create the arrays for the Complex 
#   portions of the Agency benchmark and populate them. 
# 
################################################################################
#
#  Useful shell script functions. 
#
set -x
#
source ../../Utils/Utils.sh 
#
#
################################################################################
# 
#  Hygiene - drop the load arrays and the target arrays. 
#
################################################################################
#
CMD_HYGIENE_CRV_DATA="DROP ARRAY CRV_DATA;"
exec_aql_query "${CMD_HYGIENE_CRV_DATA}"
CMD_HYGIENE_CRV_LABEL_NAMES="DROP ARRAY CRV_LABELS;"
exec_aql_query "${CMD_HYGIENE_CRV_LABEL_NAMES}"
CMD_HYGIENE_DRV_DATA="DROP ARRAY DRV_DATA;"
exec_aql_query "${CMD_HYGIENE_DRV_DATA}"
CMD_HYGIENE_DRV_LABELS="DROP ARRAY DRV_LABELS;"
exec_aql_query "${CMD_HYGIENE_DRV_LABELS}"
#
################################################################################
#
#  Create - create the load arrays and data arrays. 
#
#  The file we're working with is the KDDnet_n000001M_d000100_H.csv file. 
# It has 112 attributes, with the header yielding the following names. An 
# eyeball at the types in the file suggests the following typing. 
#
#    i                            : int32,
#    Protocol_type                : string,
#    service                      : string,
#    flag                         : string,
#    attack                       : string,
#    normalYN                     : double,
#    is_host_login                : double,
#    is_guest_login               : double,
#    logged_in                    : double,
#    land                         : double,
#    duration                     : double,
#    src_bytes                    : double,
#    dst_bytes                    : double,
#    wrong_fragment               : double,
#    urgent                       : double,
#    hot                          : double,
#    num_failed_logins            : double,
#    num_compromised              : double,
#    root_shell                   : double,
#    su_attempted                 : double,
#    num_root                     : double,
#    num_file_creations           : double,
#    num_shells                   : double,
#    num_access_files             : double,
#    num_outbound_cmds            : double,
#    count                        : double,
#    srv_count                    : double,
#    serror_rate                  : double,
#    srv_serror_rate              : double,
#    rerror_rate                  : double,
#    srv_rerror_rate              : double,
#    same_srv_rate                : double,
#    diff_srv_rate                : double,
#    srv_diff_host_rate           : double,
#    dst_host_count               : double,
#    dst_host_srv_count           : double,
#    dst_host_same_srv_rate       : double,
#    dst_host_diff_srv_rate       : double,
#    dst_host_same_src_port_rate  : double,
#    dst_host_srv_diff_host_rate  : double,
#    dst_host_serror_rate         : double,
#    dst_host_srv_serror_rate     : double,
#    dst_host_rerror_rate         : double,
#    dst_host_srv_rerror_rate     : double,
#    duration2                    : double,
#    src_bytes2                   : double,
#    dst_bytes2                   : double,
#    wrong_fragment2              : double,
#    urgent2                      : double,
#    hot2                         : double,
#    num_failed_logins2           : double,
#    num_compromised2             : double,
#    root_shell2                  : double,
#    su_attempted2                : double,
#    num_root2                    : double,
#    num_file_creations2          : double,
#    num_shells2                  : double,
#    num_access_files2            : double,
#    num_outbound_cmds2           : double,
#    count2                       : double,
#    srv_count2                   : double,
#    serror_rate2                 : double,
#    srv_serror_rate2             : double,
#    rerror_rate2                 : double,
#    srv_rerror_rate2             : double,
#    same_srv_rate2               : double,
#    diff_srv_rate2               : double,
#    srv_diff_host_rate2          : double,
#    dst_host_count2              : double,
#    dst_host_srv_count2          : double,
#    dst_host_same_srv_rate2      : double,
#    dst_host_diff_srv_rate2      : double,
#    dst_host_same_src_port_rate2 : double,
#    dst_host_srv_diff_host_rate2 : double,
#    dst_host_serror_rate2        : double,
#    dst_host_srv_serror_rate2    : double,
#    dst_host_rerror_rate2        : double,
#    dst_host_srv_rerror_rate2    : double,
#    duration3                    : double,
#    src_bytes3                   : double,
#    dst_bytes3                   : double,
#    wrong_fragment3              : double,
#    urgent3                      : double,
#    hot3                         : double,
#    num_failed_logins3           : double,
#    num_compromised3             : double,
#    root_shell3                  : double,
#    su_attempted3                : double,
#    num_root3                    : double,
#    num_file_creations3          : double,
#    num_shells3                  : double,
#    num_access_files3            : double,
#    num_outbound_cmds3           : double,
#    count3                       : double,
#    srv_count3                   : double,
#    serror_rate3                 : double,
#    srv_serror_rate3             : double,
#    rerror_rate3                 : double,
#    srv_rerror_rate3             : double,
#    same_srv_rate3               : double,
#    diff_srv_rate3               : double,
#    srv_diff_host_rate3          : double,
#    dst_host_count3              : double,
#    dst_host_srv_count3          : double,
#    dst_host_same_srv_rate3      : double,
#    dst_host_diff_srv_rate3      : double,
#    dst_host_same_src_port_rate3 : double,
#    dst_host_srv_diff_host_rate3 : double,
#    dst_host_serror_rate3        : double,
#    dst_host_srv_serror_rate3    : double,
#    dst_host_rerror_rate3        : double,
#    dst_host_srv_rerror_rate3    : double 
#
#  Now ... the way this data really works is that each of the "attributes" is 
# what we (in SciDB) would call an index or element in a dimension. So to 
# load this data, and to make sense of it, we first need to convert it into 
# a triple of: 
# 
#  LOAD_CRV < row_number, attr_num, value > [ RowNum ];
#
#   Then we convert it into: 
#
#  DATA_CRV < value : double > [ row_number, attr_num ];
#
#   We also need to break out the list of the per-attr_num labels. This 
#  can be organized as: 
#
#  LABEL_CRV < label : string > [ attr_num ] 
#
#   Now, all of the continuous random variables are doubles. But we need to 
#  break out the continuous RVs. We will also need to load the names (labels)
#  that correspond to each attr_num.
#
#  LOAD_DRV < row_number, attr_num, value : string > [ RowNum ];
#
#  DATA_DRV < value : string > [ row_number, attr_num ];
#
#   We will have the same number of rows, so the cross_join should be pretty 
#  efficient. 
#
#  LABEL_DRV < label : string > [ attr_num ] 
#
CMD_CREATE_CRV_DATA="
CREATE ARRAY CRV_DATA
<
    value : float 
>
[ row_num = 0:*,10000,0, attr_num=0:*, 110, 0 ]
"
exec_aql_query "${CMD_CREATE_CRV_DATA}"
#
#
#   This "array" is only here for it's shape. When I want to limit the 
#  number of attributes used in the workload queries -- Q5's k-means in 
#  particular -- I'll use this to organize the (small, n = 5) list of 
#  attributes to focus on. 
CMD_CREATE_CRV_ATTRIB_DIMENSION="
CREATE ARRAY ATTR_NUMS 
<
  val : int32 
> 
[ attr_num=0:*, 110, 0 ]
"
#
CMD_CREATE_CRV_CONNECTIONS_DIMENSION="
<
  v : int32 
>
[ row_num = 0:*,10000,0 ]
"
#
CMD_CREATE_CRV_LABELS="
CREATE ARRAY CRV_LABELS
<
   label : string 
>
[ attr_num=0:109, 110, 0]
"
exec_aql_query "${CMD_CREATE_CRV_LABELS}"
#
#------------------------------------------------------------------------------
#
CMD_CREATE_DRV_DATA="
CREATE ARRAY DRV_DATA
<
  value : string 
> 
[ row_num = 0:*,100000,0, attr_num=0:4, 5, 0 ]
"
exec_aql_query "${CMD_CREATE_DRV_DATA}"
#
CMD_CREATE_DRV_LABELS="
CREATE ARRAY DRV_LABELS
<
   label : string 
>
[ attr_num=0:4, 5, 0]
"
exec_aql_query "${CMD_CREATE_DRV_LABELS}"
#
################################################################################
#
#  Load - load the CRV 1D data. 
#
#  Load the CRV Label Data 
rm -rf /tmp/load.pipe
mkfifo /tmp/load.pipe
cat ../data/KDDnet_Continuous_Variable_Labels.csv | csv2scidb -c 2000000 -p NS > /tmp/load.pipe& 
#
CMD_LOAD_CRV_LABELS="
store ( 
  redimension ( 
    input ( < attr_num : int64, label : string > [Row], 
            '/tmp/load.pipe' 
          ),
    CRV_LABELS
  ),
  CRV_LABELS
)
"
exec_afl_query "${CMD_LOAD_CRV_LABELS}" -n 
#
#  Load the CRV Values Data 
rm -rf /tmp/load.pipe
mkfifo /tmp/load.pipe
# cat ../data/KDDnet_1M.bin > /tmp/load.pipe & 
# cat ../data/KDD_CRV_100M.csv | csv2scidb -c 1000000 -p NNN > /tmp/load.pipe & 
# gunzip -c /datadisk1/data/Complex/CRV_DATA_100M.bin.gz > /tmp/load.pipe &
#
cat /public/data/Agency/Complex/data/KDDnet_CRV_100K.bin >  /tmp/load.pipe &
#   Option # 1: 
#  CMD_LOAD_CRV_DATA="load ( CRV_LOAD, '/tmp/load.pipe')"
#  exec_afl_query "${CMD_LOAD_CRV_DATA}" -n 
#
#  Binary Load
CMD_LOAD_CRV_DATA_BINARY="
store ( 
  redimension ( 
    project ( 
      apply ( 
        input ( 
            < i_row_num : int64, i_attr_num : int64, i_value : float > [Row], 
            '/tmp/load.pipe',
            0,
            '(int64,int64,float)'
          ),
          row_num, (i_row_num-1),
          attr_num, i_attr_num,
          value, i_value
      ),
      row_num, attr_num, value 
    ),
    CRV_DATA
  ),
  CRV_DATA
)
"
exec_afl_query_wd "${CMD_LOAD_CRV_DATA_BINARY}" -n 
#
#  Text Load 
CMD_LOAD_CRV_DATA_TEXT="
store ( 
  redimension ( 
    project ( 
      apply ( 
        input ( 
            < i_row_num : int32, i_attr_num : int32, i_value : int32 > [Row], 
            '/tmp/load.pipe'
          ),
          row_num, int64(i_row_num) - 1,
          attr_num, int64(i_attr_num) - 1,
          value, double(i_value)
      ),
      row_num, attr_num, value 
    ),
    CRV_DATA
  ),
  CRV_DATA
)
"
# exec_afl_query_wd "${CMD_LOAD_CRV_DATA_BINARY}" -n 
exec_afl_query_wd "${CMD_LOAD_CRV_DATA_TEXT}" -n 
#
exit;
#  6:18 - 1M cells x 108 attributes. 
#
#------------------------------------------------------------------------------
#
#  Load the DRV Label data 
rm -rf /tmp/load.pipe
mkfifo /tmp/load.pipe
cat ../data/KDDnet_Discrete_Variable_Labels.csv | csv2scidb -c 2000000 -p NS > /tmp/load.pipe& 
CMD_LOAD_DRV_LABELS="
store (
  redimension (
    input ( < attr_num : int64, label : string > [Row], '/tmp/load.pipe' ),
    DRV_LABELS
  ),
  DRV_LABELS
)
"
exec_afl_query "${CMD_LOAD_DRV_LABELS}" -n 
#
#  Load the DRV Values Data 
rm -rf /tmp/load.pipe
mkfifo /tmp/load.pipe
cat ../data/KDDnet_Discrete_Variable_Values.csv | csv2scidb -c 2000000 -p NNN > /tmp/load.pipe& 
CMD_LOAD_DRV_DATA="
store (
  redimension (
    apply ( 
      input ( < i_row_num : int64, i_attr_num : int64, value : string > [Row],
              '/tmp/load.pipe'
            ),
      row_num, i_row_num - 1,
      attr_num, i_attr_num - 1
    ),
    DRV_DATA
  ),
  DRV_DATA
)
"
exec_afl_query "${CMD_LOAD_DRV_DATA}" -n
#
#  6:22  - 1M cells x 4 attributes 
#
################################################################################
#  Check that what you got makes sense. . . 
#
for i in CRV_DATA DRV_DATA; do 
  echo "+--------------------"
  echo "| Details for ${i} |"
  echo "+--------------------"

  CMD_CHECK_CHUNKS="SELECT * FROM array_chunk_details ('${i}')"
  exec_aql_query "${CMD_CHECK_CHUNKS}"
 
  CMD_CHECK_COUNT="SELECT COUNT(*) FROM ${i}"
  exec_aql_query "${CMD_CHECK_COUNT}"
 
  CMD_CHECK_SIZE_AND_SHAPE="SELECT * FROM dimensions ( ${i} )"
  exec_aql_query "${CMD_CHECK_SIZE_AND_SHAPE}"

done 
#
################################################################################

