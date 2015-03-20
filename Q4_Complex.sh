#!/bin/bash
#
#   File:   Q4_Complex.sh
#
#  About: 
#
#------------------------------------------------------------------------------
#
#  Q4: Classification:  
#
#      Predict bad/good connection based on network time measurements. Compute 
#      a Naïve Bayes classifier for the two classes based on numeric features 
#      (e.g. exclude categorical attributes). This task requires creating the 
#      target “class” variable, by transformiong the input data set.
#
#------------------------------------------------------------------------------
#
#  Naive Bayesian Classifiers: 
#
#   For a useful intro and overview .... 
#
#   http://www.inf.u-szeged.hu/~ormandi/ai2/06-naiveBayes-example.pdf
#
#   The basic idea is to compute a function that takes as input the vector 
#  of attribute values and computes the probability that an output is 
#  'true' or 'false'. In other words, if I look at a collection of values 
#  for { attr1, attr2, ... attrn }, what is the probability that the 
#  normalYN value will be true (1) or false (0)?
#
#   Our training data looks like this: 
#
#             \  attribute
#  connections \     normalYN   attr1   attr2   attr3   attr4         attrn
#               \  +----------+-------+-------+-------+-------+     +-------+
#      C1          |          |       |       |       |       | ... |       |
#                  +----------+-------+-------+-------+-------+     +-------+
#      C2          |          |       |       |       |       | ... |       |
#                  +----------+-------+-------+-------+-------+     +-------+
#     ...             ...        ...      ...    ...     ...    ...    ...
#                  +----------+-------+-------+-------+-------+     +-------+
#      Cm          |          |       |       |       |       | ... |       |
#                  +----------+-------+-------+-------+-------+     +-------+
#
#   The normalYN is an double, that has a ( 0 / 1 ) value, but we can turn this
#  into a vector of integers without a problem. 
#
#   The other attributes are all doubles as well.
#
#   In formal terms, the classifier to Vb from A[0...n] is: 
#                                     _________
#                                       |   |
#   Vb  = argmax            P ( Vj )    |   |  P ( Ai | Vj )     --- ( 1 ) 
#               ( Vj in V )             |   |
# 
#   ... where P ( Ai | Vj ) is estimated as ....
#
#                          Nc + M . p 
#    P ( Ai | Vj ) =    -------------                            --- ( 2 ) 
#                          N  + M 
#
#   ... where ....
#
#        N  = 'number of training samples for which v = Vj'
#        Nc = 'number of training samples for which v = Vj when a = Ai'
#        p  = 'prior estimate for P ( Ai | Vj )' 
#        M  = 'the equivalent sample size' 
#
#   Now, as (in this case) the attributes attr[0...n] are continuous, we 
#  need to convert it into a P() by making the assumption that the 
#  values in these variables are normally distributed. This is a very bad 
#  assumption, and obviously not true in this case. Never-the-less ... 
#
#    What's the shape of the array? It's going to be [ NormalNY, i, j ]. 
#
#    Step 1: Compute E[Ai], and VAR[Ai] for each Ai in A, for each outcome 
#            value of normalYN. 
#            
#
#------------------------------------------------------------------------------
#
source ../../Utils/Utils.sh 
#
#  Classification: Predict bad/good connection based on network time 
# measurements. Compute a Naïve Bayes classifier for the two classes based on 
# numeric features (e.g. exclude categorical attributes). This task requires 
# creating the target “class” variable, by transforming the input data set.
#
#------------------------------------------------------------------------------
#
CMD_NORMAL_YN_PREDICT_QUERY="
redimension (
  apply (
    aggregate (
      apply (
        redimension (
          apply (
            cross_join (
              CRV_DATA AS K,
              redimension ( 
                cross_join ( 
                  cross_join (
                    CRV_DATA AS D, 
                    project ( 
                      apply ( 
                        slice ( CRV_DATA, attr_num, 2 ),
                        yn, floor(value)
                      ), yn
                    ) AS NY,
                    D.row_num, NY.row_num
                  ) AS DYN,
                  project ( 
                    apply ( 
                      filter ( 
                        dimensions ( CRV_DATA ), 
                        name = 'attr_num' 
                      ), 
                      n, 1.0 / double(high - low)
                    ), n
                  ) AS P
                ),
                < pi : double null, mu : double null, sigma : double null > 
                [ attr_num = 0:*,1000,0, yn=0:1,2,0 ],
                sum ( P.n ) AS pi,
                avg ( DYN.value ) AS mu,
                stdev ( DYN.value ) AS sigma
              ) AS NB,
              K.attr_num, NB.attr_num
            ),
            t, iif ( sigma > 0,
                     log(NB.pi)-0.5*
                     (K.value-NB.sigma)*(K.value-NB.sigma)/(NB.sigma*NB.sigma),
                     0
                   )
          ),
          < p : double null >
          [ row_num=0:*,1000,0, yn=0:1,2,0 ],
          sum ( t ) AS p
        ),
        normalYNeq0, iif ( yn = 0, p, 0 ),
        normalYNeq1, iif ( yn = 1, p, 0 )
      ),
      sum ( normalYNeq0 ) AS sum_normalYNeq0,
      sum ( normalYNeq1 ) AS sum_normalYNeq1,
      row_num
    ),
    normalYN_predicted, iif(sum_normalYNeq0<=sum_normalYNeq1,1,0)
  ),
  < CNT : uint64 NULL >
  [ normalYN_predicted=0:1,2,0 ],
  COUNT(*) AS CNT
);
"
#
exec_afl_query_wd "${CMD_NORMAL_YN_PREDICT_QUERY}"
#
exit;
#
#------------------------------------------------------------------------------
#
#   Question: How did Vertica arrive at its list of numeric variables? 
#   Question: What is 'c'?
# 
#  The Vertica implementation looks like this ... 
#
#  CREATE TABLE KDDnet_n000100K_d000100_NB (
#     i int                            * -- count(i) = n
#    ,Protocol_type char(10)
#    ,service       char(10)
#    ,flag          char(10)
#    ,attack        char(12)
#    ,normalYN      int                GB
#    ,j             int                GB
#    ,c             char(32)
#    ,v             float              *
#  );
#  
#  Q1:   Model Query 
#
#  SELECT 
#         normalYN
#         ,j
#         ,sum(1.0/T.n)   AS pi
#         ,avg(v)         AS mu
#         ,stddev(v)      AS sigma
#    INTO NB
#    FROM  KDDnet_n000100K_d000100_NB
#         ,(SELECT count(distinct i) AS n FROM KDDnet_n000100K_d000100_NB)T
#   GROUP BY normalYN,j;
#
#   Q2:   Scoring Query 
#
#  SELECT 
#         i
#         ,NB.normalYN
#         ,sum(case when sigma>0 
#                   then log(pi) -0.5*(v-sigma)*(v-sigma)/(sigma*sigma)
#                   else 0
#              end) AS p
#   INTO X_P
#   FROM KDDnet_n000100K_d000100_NB K JOIN NB on K.j=NB.j
#  GROUP BY i,NB.normalYN;
#
#   Q3:   Pivot Query 
#
#    SELECT 
#           i
#           ,CASE WHEN normalYNeq0<=normalYNeq1 
#                 THEN 1
#                 ELSE 0
#             END AS normalYN_predicted
#      INTO X_NB
#       FROM (SELECT
#                    i
#                    ,sum(case when normalYN=0 then p end) AS normalYNeq0
#                    ,sum(case when normalYN=1 then p end) AS normalYNeq1
#               FROM X_P
#              GROUP BY i);
#
#   Q4:   Test 
#
#   SELECT normalYN_predicted,
#          count(*) 
#     FROM X_NB 
#    GROUP BY normalYN_predicted;
#
#------------------------------------------------------------------------------
#
#   Sanity check: how many of the rows fall into each NormalNY ... 
#
CMD_QUERY_SANITY_CHECK_ON_YN="
redimension ( 
  project ( 
    apply ( 
      slice ( CRV_DATA, attr_num, 2 ),
      i_norm, floor(value)
    ),
    i_norm
  ),
  < CNT : uint64 NULL > [ i_norm=0:*,10,0 ],
  count(*) AS CNT
)
"
exec_afl_query_wd "${CMD_QUERY_SANITY_CHECK_ON_YN}"
#
#  {i_norm} CNT
#  {0} 78260000     Normal?
#  {1} 21740000     Not Normal?
# 
#------------------------------------------------------------------------------
#
#   Q0: Hygiene. Delete all of the intermediate arrays ... 
#
CMD_HYGIENE="remove ( NB )"
exec_afl_query_wd "${CMD_HYGIENE}"
CMD_HYGIENE="remove ( XP )"
exec_afl_query_wd "${CMD_HYGIENE}"
CMD_HYGIENE="remove ( X_NB )"
exec_afl_query_wd "${CMD_HYGIENE}"
#
#   Q1: Model query ... 
#
#  SELECT 
#         normalYN
#         ,j
#         ,sum(1.0/T.n)   AS pi
#         ,avg(v)         AS mu
#         ,stddev(v)      AS sigma
#    INTO NB
#    FROM  KDDnet_n000100K_d000100_NB
#         ,(SELECT count(distinct i) AS n FROM KDDnet_n000100K_d000100_NB)T
#   GROUP BY normalYN,j;
CMD_MODEL_QUERY="
store ( 
  redimension ( 
    cross_join ( 
      cross_join (
        CRV_DATA AS D, 
        project ( 
          apply ( 
            slice ( CRV_DATA, attr_num, 2 ),
            yn, floor(value)
          ),
          yn
        ) AS NY,
        D.row_num, NY.row_num
      ) AS DYN,
      project ( 
        apply ( 
          filter ( 
            dimensions ( CRV_DATA ), 
            name = 'attr_num' 
          ), 
          n, 1.0 / double(high - low)
        ),
        n
      ) AS P
    ),
    < pi : double null, mu : double null, sigma : double null > 
    [ attr_num = 0:*,1000,0, yn=0:1,2,0 ],
    sum ( P.n ) AS pi,
    avg ( DYN.value ) AS mu,
    stdev ( DYN.value ) AS sigma
  ),
  NB
)"
#
exec_afl_query_wd "${CMD_MODEL_QUERY}"
#
#
#  Q2: Scoring Query ... 
#  SELECT 
#         i
#         ,NB.normalYN
#         ,sum(case when sigma>0 
#                   then log(pi) -0.5*(v-sigma)*(v-sigma)/(sigma*sigma)
#                   else 0
#              end) AS p
#   INTO X_P
#   FROM KDDnet_n000100K_d000100_NB K JOIN NB on K.j=NB.j
#  GROUP BY i,NB.normalYN;
#
CMD_SCORING_QUERY=
store ( 
  redimension ( 
    apply ( 
      cross_join ( 
        CRV_DATA AS K,
        NB,
        K.attr_num, NB.attr_num
      ),
      t, iif ( sigma > 0, 
               log(NB.pi)-
               0.5*(K.value-NB.sigma)*(K.value-NB.sigma)/(NB.sigma*NB.sigma), 
               0 
             )
    ),
    < p : double null > 
    [ row_num=0:*,1000,0, yn=0:1,2,0 ],
    sum ( t ) AS p 
  ),
  X_P
)
"
exec_afl_query_wd "${CMD_SCORING_QUERY}
#
#   Q3:   Pivot Query 
#
#    SELECT 
#           i
#           ,CASE WHEN normalYNeq0<=normalYNeq1 
#                 THEN 1
#                 ELSE 0
#             END AS normalYN_predicted
#      INTO X_NB
#       FROM (SELECT
#                    i
#                    ,sum(case when normalYN=0 then p end) AS normalYNeq0
#                    ,sum(case when normalYN=1 then p end) AS normalYNeq1
#               FROM X_P
#              GROUP BY i);
#
CMD_PIVOT_QUERY="
store ( 
  apply ( 
    aggregate (
      apply ( 
        X_P,
        normalYNeq0, iif ( yn = 0, p, 0 ),
        normalYNeq1, iif ( yn = 1, p, 0 )
      ),
      sum ( normalYNeq0 ) AS sum_normalYNeq0,
      sum ( normalYNeq1 ) AS sum_normalYNeq1,
      row_num
    ), 
    normalYN_predicted, iif(sum_normalYNeq0<=sum_normalYNeq1,1,0)
  ), 
  X_NB
)
"
exec_afl_query_wd "${CMD_PIVOT_QUERY}
#
#   Q4:   Test 
#
#   SELECT normalYN_predicted,
#          count(*) 
#     FROM X_NB 
#    GROUP BY normalYN_predicted;
#
CMD_TEST_QUERY="
redimension ( 
  X_NB,
  < CNT : uint64 NULL >
  [ normalYN_predicted=0:1,2,0 ],
  COUNT(*) AS CNT 
)
"
exec_afl_query_wd "${CMD_TEST_QUERY}"
#
#------------------------------------------------------------------------------

