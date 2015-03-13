## Setup for SciDB on NERSC

ulimit -a
ulimit -aH

onhead_startscidb.sh 

source /dev/shm/scidb/setup.sh

##
## 
##
## About:
##
##    This set of AFL/AQL instructions duplicates the contents of the 
##  minimal CRV_DATA multiple times to get to up to 1,000,000,000 rows in 
##  the array used by the queries (CRV_DATA). We start with an array 
##  that has 100,000 "observation" rows and 108 "attribute" columns. 
##
##  This granule size array has the following shape: 
##
##  CRV_DATA<value:double> [row_num=0:*,1000,0,attr_num=0:*,1000,0]
##
##  NOTE: This is loaded into SciDB using the Create_and_Load.sh script. 
##        The base data is located at: 
##
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA;"
## {i} MAX
## {0} 99999
##
##############################################################################
##
##   1. Scale from 100,000 to 1,000,000 rows. 
##
##  Create the target array. Has the same size and shape as the CRV_DATA
## array, but it will (of course) contain more data. 
##
time iquery -q "DROP ARRAY CRV_DATA_1M_TEST;"
CREATE_1M_TEST="
CREATE ARRAY CRV_DATA_1M_TEST
  < value : float> 
  [ row_num=0:*,1000,0, attr_num=0:*,1000,0]
"
time iquery -q "${CREATE_1M_TEST}"
##
##  Initialize the 1M_TEST array with the first rows from the CRV_DATA 
## load array. 
time iquery -q "SET LANG AQL"
time iquery -q "SET FETCH;"
time iquery -q "SELECT * FROM op_now()"
##
time iquery -q "SET NO FETCH"
SEL_VALUE_INTO_TEST="
  SELECT value
    INTO CRV_DATA_1M_TEST
    FROM between ( CRV_DATA, 0, 0, 99999, null )
  "
time iquery -q "${SEL_VALUE_INTO_TEST}"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
##  Check #1 - should be 10,800,000 cells, and max of 99999
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT COUNT(*) FROM CRV_DATA_1M_TEST"
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA_1M_TEST"
##
## Double the size once. . . 
#SET LANG AQL;
#SET FETCH;
#SELECT * FROM op_now();
##
#time iquery -q "SET LANG AFL"
#SET NO FETCH;
INSERT_INTO_CRV_DATA="
  insert ( 
    cast ( 
      substitute ( 
        ( SELECT MIN ( value ) AS value 
            FROM  ( SELECT row_num + ( 1 * 100000 ) as new_row_num,
                           attr_num AS new_attr_num,
                           value
                      FROM between ( CRV_DATA_1M_TEST, 0, 0, null, null )
                  )
          REDIMENSION BY [ new_row_num=0:*,1000,0,new_attr_num=0:*,1000,0 ]
        ),
        build ( < val : float > [M=0:0,1,0], '[(0.0)]', true )
      ),
      CRV_DATA_1M_TEST
    ),
    CRV_DATA_1M_TEST
  )
"
time iquery -anq "${INSERT_INTO_CRV_DATA}"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
##  Check #2 - should be 21,600,000, and 199,999 max. 
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT COUNT(*) FROM CRV_DATA_1M_TEST"
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA_1M_TEST"

#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
##  Double the size a second time ... 
#SET LANG AFL;
#SET NO FETCH;
INSERT_INTO_CRV_DATA="
   insert ( 
    cast ( 
      substitute ( 
        ( SELECT MIN ( value ) AS value 
            FROM  ( SELECT row_num + ( 2 * 100000 ) as new_row_num,
                           attr_num AS new_attr_num,
                           value
                      FROM between ( CRV_DATA_1M_TEST, 0, 0, null, null )
                  )
          REDIMENSION BY [ new_row_num=0:*,1000,0,new_attr_num=0:*,1000,0 ]
        ),
        build ( < val : float > [M=0:0,1,0], '[(0.0)]', true )
      ),
      CRV_DATA_1M_TEST
    ),
    CRV_DATA_1M_TEST
  )
"
time iquery -aq "${INSERT_INTO_CRV_DATA}"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"

##
##  Check #3 - Should be 43,200,000, and 399,999 max. 
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT COUNT(*) FROM CRV_DATA_1M_TEST"
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA_1M_TEST"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
##  Double it a third time, to get to 800,000 rows ... 
#SET LANG AFL;
#SET NO FETCH;
INSERT_INTO_CRV_DATA="
   insert ( 
    cast ( 
      substitute ( 
        ( SELECT MIN ( value ) AS value 
            FROM  ( SELECT row_num + ( 4 * 100000 ) as new_row_num,
                           attr_num AS new_attr_num,
                           value
                      FROM between ( CRV_DATA_1M_TEST, 0, 0, null, null )
                  )
          REDIMENSION BY [ new_row_num=0:*,1000,0,new_attr_num=0:*,1000,0 ]
        ),
        build ( < val : float > [M=0:0,1,0], '[(0.0)]', true )
      ),
      CRV_DATA_1M_TEST
    ),
    CRV_DATA_1M_TEST
  )
"
time iquery -anq "${INSERT_INTO_CRV_DATA}"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
##  Check #4 - Should be 86,400,000, and 799,999 max. 
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT COUNT(*) FROM CRV_DATA_1M_TEST"
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA_1M_TEST"
##
##  Last addition. In this pass, we only want to add 2,000,000 rows this time. 
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
#SET LANG AFL;
#SET NO FETCH;
INSERT_INTO_CRV_DATA="
   insert ( 
    cast ( 
      substitute ( 
        ( SELECT MIN ( value ) AS value 
            FROM  ( SELECT row_num + ( 8 * 25000 ) as new_row_num,
                           attr_num AS new_attr_num,
                           value
                      FROM between ( CRV_DATA_1M_TEST, 0, 0, null, null )
                  )
          REDIMENSION BY [ new_row_num=0:*,1000,0,new_attr_num=0:*,1000,0 ]
        ),
        build ( < val : float > [M=0:0,1,0], '[(0.0)]', true )
      ),
      CRV_DATA_1M_TEST
    ),
    CRV_DATA_1M_TEST
  )
"
time iquery -anq "${INSERT_INTO_CRV_DATA}"
##
#SET LANG AQL;
#SET FETCH;
time iquery -q "SELECT * FROM op_now()"
##
## Final check - Should be 108,000,000, and 999,999 max. 
#107,998,920
#SET LANG AQL;
#SET FETCH;


##
##  Rename the CRV_DATA_1M_TEST to CRV_DATA_1M
#
##### EVENTUALLY change back but
# renaming some data so we can test iteratively!
# CRV_DATA --> CRV_DATA_100K
# CRV_DATA_1M_TEST --> CRV_DATA
# Then we can run the queries again on the larger dataset!
#time iquery -q "RENAME ARRAY CRV_DATA_1M_TEST TO CRV_DATA_1M"
time iquery -q "DROP ARRAY CRV_DATA_100K"
time iquery -q "RENAME ARRAY CRV_DATA TO CRV_DATA_100K"
time iquery -q "DROP ARRAY CRV_DATA"
time iquery -q "RENAME ARRAY CRV_DATA_1M_TEST TO CRV_DATA"
##

echo "Last check:"
time iquery -q "SELECT COUNT(*) FROM CRV_DATA"
time iquery -q "SELECT MAX ( row_num ) FROM CRV_DATA"

onhead_stopscidb.sh