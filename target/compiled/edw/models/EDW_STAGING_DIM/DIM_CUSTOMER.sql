 

----SRC LAYER----
WITH
SRC_IW as ( SELECT *     from      EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_EMP as ( SELECT *     from      EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_PROV as ( SELECT *      from      EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_NET as ( SELECT *      from      EDW_STAGING_DIM.DIM_NETWORK ),
SRC_REP as ( SELECT *      from      EDW_STAGING_DIM.DIM_REPRESENTATIVE )
//SRC_IW as ( SELECT *     from      DIM_INJURED_WORKER),
//SRC_EMP as ( SELECT *     from      DIM_EMPLOYER),
//SRC_PROV as ( SELECT *      from     DIM_PROVIDER),
//SRC_NET as ( SELECT *      from     DIM_NETWORK),
//SRC_REP as ( SELECT *      from      DIM_REPRESENTATIVE )


----LOGIC LAYER----
,

LOGIC_EMP as ( SELECT 
		  EMPLOYER_HKEY                            AS                              EMPLOYER_HKEY
        , CUSTOMER_NUMBER::VARCHAR()               AS                            CUSTOMER_NUMBER
        , BUSINESS_NAME                            AS                              BUSINESS_NAME
        , PRIMARY_BUSINESS_PHONE_NUMBER            AS              PRIMARY_BUSINESS_PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         DOCUMENT_BLOCK_IND
        , PHYSICAL_ADDRESS_LINE_1                  AS                    PHYSICAL_ADDRESS_LINE_1
        , PHYSICAL_ADDRESS_LINE_2                  AS                    PHYSICAL_ADDRESS_LINE_2
        , PHYSICAL_ADDRESS_CITY                    AS                      PHYSICAL_ADDRESS_CITY
        , PHYSICAL_ADDRESS_STATE_CODE              AS                PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE             AS               PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY                  AS                    PHYSICAL_ADDRESS_COUNTY
        , MAIL_ADDRESS_LINE_1                      AS                        MAIL_ADDRESS_LINE_1
        , MAIL_ADDRESS_LINE_2                      AS                        MAIL_ADDRESS_LINE_2
        , MAIL_ADDRESS_CITY                        AS                          MAIL_ADDRESS_CITY
        , MAIL_ADDRESS_STATE_CODE                  AS                    MAIL_ADDRESS_STATE_CODE
        , MAIL_ADDRESS_POSTAL_CODE                 AS                   MAIL_ADDRESS_POSTAL_CODE
        , NULL                                     AS                   MAIL_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
		, NVL2(CUSTOMER_NUMBER,'Y','N')            AS                               EMPLOYER_IND
			from SRC_EMP
            ),   
LOGIC_IW as ( SELECT 
		  INJURED_WORKER_HKEY                      AS                        INJURED_WORKER_HKEY
        , CUSTOMER_NUMBER::VARCHAR()               AS                            CUSTOMER_NUMBER
        , FULL_NAME                                AS                                  FULL_NAME
        , NULL                                     AS                               PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         DOCUMENT_BLOCK_IND
        , PHYSICAL_STREET_ADDRESS_1                AS                  PHYSICAL_STREET_ADDRESS_1
        , PHYSICAL_STREET_ADDRESS_2                AS                  PHYSICAL_STREET_ADDRESS_2
        , PHYSICAL_ADDRESS_CITY_NAME               AS                 PHYSICAL_ADDRESS_CITY_NAME
        , PHYSICAL_ADDRESS_STATE_CODE              AS                PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE             AS               PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY_NAME             AS               PHYSICAL_ADDRESS_COUNTY_NAME
        , MAILING_STREET_ADDRESS_1                 AS                   MAILING_STREET_ADDRESS_1
        , MAILING_STREET_ADDRESS_2                 AS                   MAILING_STREET_ADDRESS_2
        , MAILING_ADDRESS_CITY_NAME                AS                  MAILING_ADDRESS_CITY_NAME
        , MAILING_ADDRESS_STATE_CODE               AS                 MAILING_ADDRESS_STATE_CODE
        , MAILING_ADDRESS_POSTAL_CODE              AS                MAILING_ADDRESS_POSTAL_CODE
        , MAILING_ADDRESS_COUNTY_NAME	           AS                MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
	    , NVL2(CUSTOMER_NUMBER,'Y','N')            AS                         INJURED_WORKER_IND
			from SRC_IW
            ),			

LOGIC_REP as ( SELECT 
          REPRESENTATIVE_HKEY                       AS                       REPRESENTATIVE_HKEY
        , CUSTOMER_NUMBER::VARCHAR()                AS                      	 CUSTOMER_NUMBER
        , CUSTOMER_NAME                             AS                             CUSTOMER_NAME
        , PRIMARY_BUSINESS_PHONE_NUMBER             AS             PRIMARY_BUSINESS_PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                        AS                        DOCUMENT_BLOCK_IND
        , PHYSICAL_STREET_ADDRESS_1                 AS                 PHYSICAL_STREET_ADDRESS_1
        , PHYSICAL_STREET_ADDRESS_2                 AS                 PHYSICAL_STREET_ADDRESS_2
        , PHYSICAL_ADDRESS_CITY_NAME                AS                PHYSICAL_ADDRESS_CITY_NAME
        , PHYSICAL_ADDRESS_STATE_CODE               AS               PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE              AS              PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY_NAME              AS              PHYSICAL_ADDRESS_COUNTY_NAME
        , MAILING_STREET_ADDRESS_1                  AS                  MAILING_STREET_ADDRESS_1
        , MAILING_STREET_ADDRESS_2                  AS                  MAILING_STREET_ADDRESS_2
        , MAILING_ADDRESS_CITY_NAME                 AS                 MAILING_ADDRESS_CITY_NAME
        , MAILING_ADDRESS_STATE_CODE                AS                MAILING_ADDRESS_STATE_CODE
        , MAILING_ADDRESS_STATE_NAME                AS                MAILING_ADDRESS_STATE_NAME
        , MAILING_ADDRESS_POSTAL_CODE               AS               MAILING_ADDRESS_POSTAL_CODE
        , MAILING_ADDRESS_COUNTY_NAME               AS               MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                        AS                        CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                     AS                     RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                           AS                           RECORD_END_DATE
        , LOAD_DATETIME                             AS                             LOAD_DATETIME
        , UPDATE_DATETIME                           AS                           UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                     AS                     PRIMARY_SOURCE_SYSTEM
		, 'Y'	                                    AS	                      REPRESENTATIVE_IND
			from SRC_REP
            ),


LOGIC_PROV as ( SELECT 
	      PROVIDER_HKEY                            AS                              PROVIDER_HKEY
        , PROVIDER_PEACH_NUMBER::VARCHAR()         AS                      PROVIDER_PEACH_NUMBER
        , PROVIDER_NAME                            AS                              PROVIDER_NAME
        , PROVIDER_PHONE_NUMBER                    AS                      PROVIDER_PHONE_NUMBER
        , NULL                                     AS                         DOCUMENT_BLOCK_IND
        , PRACTICE_STREET_ADDRESS_1                AS                  PRACTICE_STREET_ADDRESS_1
        , PRACTICE_STREET_ADDRESS_2                AS                  PRACTICE_STREET_ADDRESS_2
        , PRACTICE_ADDRESS_CITY                    AS                      PRACTICE_ADDRESS_CITY
        , PRACTICE_ADDRESS_STATE_CODE              AS                PRACTICE_ADDRESS_STATE_CODE
        , PRACTICE_ZIP_CODE_NUMBER                 AS                   PRACTICE_ZIP_CODE_NUMBER
        , PRACTICE_COUNTY_NAME                     AS                       PRACTICE_COUNTY_NAME
        , CORRESPONDENCE_STREET_ADDRESS_1          AS            CORRESPONDENCE_STREET_ADDRESS_1
        , CORRESPONDENCE_STREET_ADDRESS_2          AS            CORRESPONDENCE_STREET_ADDRESS_2
        , CORRESPONDENCE_ADDRESS_CITY              AS                CORRESPONDENCE_ADDRESS_CITY
        , CORRESPONDENCE_ADDRESS_STATE_CODE        AS          CORRESPONDENCE_ADDRESS_STATE_CODE
        , CORRESPONDENCE_ZIP_CODE_NUMBER           AS             CORRESPONDENCE_ZIP_CODE_NUMBER
        , CORRESPONDENCE_COUNTY_NAME               AS                 CORRESPONDENCE_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
		, NVL2(PROVIDER_PEACH_NUMBER,'Y','N')      AS	                            PROVIDER_IND
			from SRC_PROV
            ),
LOGIC_NET as ( SELECT 
		  NETWORK_HKEY                             AS                               NETWORK_HKEY
        , NVL(CORESUITE_CUSTOMER_NUMBER, NETWORK_NUMBER)::VARCHAR()                 
		                                           AS                  CORESUITE_CUSTOMER_NUMBER
        , NETWORK_NAME                             AS                               NETWORK_NAME
        , NULL                                     AS                               PHONE_NUMBER
        , NULL                                     AS                         DOCUMENT_BLOCK_IND
        , NETWORK_PHYSICAL_STREET_ADDRESS_1        AS          NETWORK_PHYSICAL_STREET_ADDRESS_1
        , NETWORK_PHYSICAL_STREET_ADDRESS_2        AS          NETWORK_PHYSICAL_STREET_ADDRESS_2
        , NETWORK_PHYSICAL_ADDRESS_CITY_NAME       AS         NETWORK_PHYSICAL_ADDRESS_CITY_NAME
        , NETWORK_PHYSICAL_ADDRESS_STATE_CODE      AS        NETWORK_PHYSICAL_ADDRESS_STATE_CODE
        , NETWORK_PHYSICAL_ADDRESS_ZIP_CODE        AS          NETWORK_PHYSICAL_ADDRESS_ZIP_CODE
        , NULL                                     AS                    PHYSICAL_ADDRESS_COUNTY
        , NETWORK_MAILING_STREET_ADDRESS_1         AS           NETWORK_MAILING_STREET_ADDRESS_1
        , NETWORK_MAILING_STREET_ADDRESS_2         AS           NETWORK_MAILING_STREET_ADDRESS_2
        , NETWORK_MAILING_ADDRESS_CITY_NAME        AS          NETWORK_MAILING_ADDRESS_CITY_NAME
        , NETWORK_MAILING_ADDRESS_STATE_CODE       AS         NETWORK_MAILING_ADDRESS_STATE_CODE
        , NETWORK_MAILING_ADDRESS_ZIP_CODE         AS           NETWORK_MAILING_ADDRESS_ZIP_CODE
        , NULL  					               AS                MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
		, 'Y'                                      AS	                             NETWORK_IND

			from SRC_NET
            )
			
----RENAME LAYER ----
,


RENAME_EMP as ( SELECT 
		  EMPLOYER_HKEY                            AS                              EMP_CUSTOMER_HKEY
        , CUSTOMER_NUMBER                          AS                            EMP_CUSTOMER_NUMBER
        , BUSINESS_NAME                            AS                              EMP_CUSTOMER_NAME
        , PRIMARY_BUSINESS_PHONE_NUMBER            AS                               EMP_PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         EMP_DOCUMENT_BLOCK_IND
        , PHYSICAL_ADDRESS_LINE_1                  AS                    EMP_PHYSICAL_ADDRESS_LINE_1
        , PHYSICAL_ADDRESS_LINE_2                  AS                    EMP_PHYSICAL_ADDRESS_LINE_2
        , PHYSICAL_ADDRESS_CITY                    AS                      EMP_PHYSICAL_ADDRESS_CITY
        , PHYSICAL_ADDRESS_STATE_CODE              AS                EMP_PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE             AS               EMP_PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY                  AS                    EMP_PHYSICAL_ADDRESS_COUNTY
        , MAIL_ADDRESS_LINE_1                      AS                     EMP_MAILING_ADDRESS_LINE_1
        , MAIL_ADDRESS_LINE_2                      AS                     EMP_MAILING_ADDRESS_LINE_2
        , MAIL_ADDRESS_CITY                        AS                       EMP_MAILING_ADDRESS_CITY
        , MAIL_ADDRESS_STATE_CODE                  AS                 EMP_MAILING_ADDRESS_STATE_CODE
        , MAIL_ADDRESS_POSTAL_CODE                 AS                EMP_MAILING_ADDRESS_POSTAL_CODE
        , MAIL_ADDRESS_COUNTY_NAME                 AS                EMP_MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         EMP_CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      EMP_RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            EMP_RECORD_END_DATE
        , LOAD_DATETIME                            AS                              EMP_LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            EMP_UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      EMP_PRIMARY_SOURCE_SYSTEM
		, EMPLOYER_IND                             AS                                   EMPLOYER_IND
			from      LOGIC_EMP
        ),
		
RENAME_REP as ( SELECT 
          REPRESENTATIVE_HKEY                       AS                             REP_CUSTOMER_HKEY
        , CUSTOMER_NUMBER                           AS                      	 REP_CUSTOMER_NUMBER
        , CUSTOMER_NAME                             AS                             REP_CUSTOMER_NAME
        , PRIMARY_BUSINESS_PHONE_NUMBER             AS                              REP_PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                        AS                        REP_DOCUMENT_BLOCK_IND
        , PHYSICAL_STREET_ADDRESS_1                 AS                   REP_PHYSICAL_ADDRESS_LINE_1
        , PHYSICAL_STREET_ADDRESS_2                 AS                   REP_PHYSICAL_ADDRESS_LINE_2
        , PHYSICAL_ADDRESS_CITY_NAME                AS                     REP_PHYSICAL_ADDRESS_CITY
        , PHYSICAL_ADDRESS_STATE_CODE               AS               REP_PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE              AS              REP_PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY_NAME              AS                   REP_PHYSICAL_ADDRESS_COUNTY
        , MAILING_STREET_ADDRESS_1                  AS                    REP_MAILING_ADDRESS_LINE_1
        , MAILING_STREET_ADDRESS_2                  AS                    REP_MAILING_ADDRESS_LINE_2
        , MAILING_ADDRESS_CITY_NAME                 AS                      REP_MAILING_ADDRESS_CITY
        , MAILING_ADDRESS_STATE_CODE                AS                REP_MAILING_ADDRESS_STATE_CODE
        , MAILING_ADDRESS_POSTAL_CODE               AS               REP_MAILING_ADDRESS_POSTAL_CODE
        , MAILING_ADDRESS_COUNTY_NAME               AS               REP_MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                        AS                        REP_CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                     AS                     REP_RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                           AS                           REP_RECORD_END_DATE
        , LOAD_DATETIME                             AS                             REP_LOAD_DATETIME
        , UPDATE_DATETIME                           AS                           REP_UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                     AS                     REP_PRIMARY_SOURCE_SYSTEM
		, REPRESENTATIVE_IND                        AS                            REPRESENTATIVE_IND
			from LOGIC_REP
            ),  
RENAME_IW as ( SELECT 
		  INJURED_WORKER_HKEY                      AS                              IW_CUSTOMER_HKEY
        , CUSTOMER_NUMBER                          AS                            IW_CUSTOMER_NUMBER
        , FULL_NAME                                AS                              IW_CUSTOMER_NAME
        , PHONE_NUMBER                             AS                               IW_PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         IW_DOCUMENT_BLOCK_IND
        , PHYSICAL_STREET_ADDRESS_1                AS                    IW_PHYSICAL_ADDRESS_LINE_1
        , PHYSICAL_STREET_ADDRESS_2                AS                    IW_PHYSICAL_ADDRESS_LINE_2
        , PHYSICAL_ADDRESS_CITY_NAME               AS                      IW_PHYSICAL_ADDRESS_CITY
        , PHYSICAL_ADDRESS_STATE_CODE              AS                IW_PHYSICAL_ADDRESS_STATE_CODE
        , PHYSICAL_ADDRESS_POSTAL_CODE             AS               IW_PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY_NAME             AS                    IW_PHYSICAL_ADDRESS_COUNTY
        , MAILING_STREET_ADDRESS_1                 AS                     IW_MAILING_ADDRESS_LINE_1
        , MAILING_STREET_ADDRESS_2                 AS                     IW_MAILING_ADDRESS_LINE_2
        , MAILING_ADDRESS_CITY_NAME                AS                       IW_MAILING_ADDRESS_CITY
        , MAILING_ADDRESS_STATE_CODE               AS                 IW_MAILING_ADDRESS_STATE_CODE
        , MAILING_ADDRESS_POSTAL_CODE              AS                IW_MAILING_ADDRESS_POSTAL_CODE
        , MAILING_ADDRESS_COUNTY_NAME              AS                IW_MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         IW_CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      IW_RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            IW_RECORD_END_DATE
        , LOAD_DATETIME                            AS                              IW_LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            IW_UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      IW_PRIMARY_SOURCE_SYSTEM
		, INJURED_WORKER_IND AS INJURED_WORKER_IND
			from      LOGIC_IW
        ),
		
RENAME_PROV as ( SELECT 
	      PROVIDER_HKEY                            AS                              CUSTOMER_HKEY
        , PROVIDER_PEACH_NUMBER                    AS                            CUSTOMER_NUMBER
        , PROVIDER_NAME                            AS                              CUSTOMER_NAME
        , PROVIDER_PHONE_NUMBER                    AS                               PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         DOCUMENT_BLOCK_IND
        , PRACTICE_STREET_ADDRESS_1                AS                    PHYSICAL_ADDRESS_LINE_1
        , PRACTICE_STREET_ADDRESS_2                AS                    PHYSICAL_ADDRESS_LINE_2
        , PRACTICE_ADDRESS_CITY                    AS                      PHYSICAL_ADDRESS_CITY
        , PRACTICE_ADDRESS_STATE_CODE              AS                PHYSICAL_ADDRESS_STATE_CODE
        , PRACTICE_ZIP_CODE_NUMBER                 AS               PHYSICAL_ADDRESS_POSTAL_CODE
        , PRACTICE_COUNTY_NAME                     AS                    PHYSICAL_ADDRESS_COUNTY
        , CORRESPONDENCE_STREET_ADDRESS_1          AS                     MAILING_ADDRESS_LINE_1
        , CORRESPONDENCE_STREET_ADDRESS_2          AS                     MAILING_ADDRESS_LINE_2
        , CORRESPONDENCE_ADDRESS_CITY              AS                       MAILING_ADDRESS_CITY
        , CORRESPONDENCE_ADDRESS_STATE_CODE        AS                 MAILING_ADDRESS_STATE_CODE
        , CORRESPONDENCE_ZIP_CODE_NUMBER           AS                MAILING_ADDRESS_POSTAL_CODE
        , CORRESPONDENCE_COUNTY_NAME               AS                MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
		, PROVIDER_IND                             AS                               PROVIDER_IND
			from      LOGIC_PROV
        ),
        
RENAME_NET as ( SELECT 
		  NETWORK_HKEY                             AS                              CUSTOMER_HKEY
        , CORESUITE_CUSTOMER_NUMBER                AS                            CUSTOMER_NUMBER
        , NETWORK_NAME                             AS                              CUSTOMER_NAME
        , PHONE_NUMBER                             AS                               PHONE_NUMBER
        , DOCUMENT_BLOCK_IND                       AS                         DOCUMENT_BLOCK_IND
        , NETWORK_PHYSICAL_STREET_ADDRESS_1        AS                    PHYSICAL_ADDRESS_LINE_1
        , NETWORK_PHYSICAL_STREET_ADDRESS_2        AS                    PHYSICAL_ADDRESS_LINE_2
        , NETWORK_PHYSICAL_ADDRESS_CITY_NAME       AS                      PHYSICAL_ADDRESS_CITY
        , NETWORK_PHYSICAL_ADDRESS_STATE_CODE      AS                PHYSICAL_ADDRESS_STATE_CODE
        , NETWORK_PHYSICAL_ADDRESS_ZIP_CODE        AS               PHYSICAL_ADDRESS_POSTAL_CODE
        , PHYSICAL_ADDRESS_COUNTY                  AS                    PHYSICAL_ADDRESS_COUNTY
        , NETWORK_MAILING_STREET_ADDRESS_1         AS                     MAILING_ADDRESS_LINE_1
        , NETWORK_MAILING_STREET_ADDRESS_2         AS                     MAILING_ADDRESS_LINE_2
        , NETWORK_MAILING_ADDRESS_CITY_NAME        AS                       MAILING_ADDRESS_CITY
        , NETWORK_MAILING_ADDRESS_STATE_CODE       AS                 MAILING_ADDRESS_STATE_CODE
        , NETWORK_MAILING_ADDRESS_ZIP_CODE         AS                MAILING_ADDRESS_POSTAL_CODE
        , MAILING_ADDRESS_COUNTY_NAME              AS                MAILING_ADDRESS_COUNTY_NAME
        , CURRENT_RECORD_IND                       AS                         CURRENT_RECORD_IND
        , RECORD_EFFECTIVE_DATE                    AS                      RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                          AS                            RECORD_END_DATE
        , LOAD_DATETIME                            AS                              LOAD_DATETIME
        , UPDATE_DATETIME                          AS                            UPDATE_DATETIME
        , PRIMARY_SOURCE_SYSTEM                    AS                      PRIMARY_SOURCE_SYSTEM
		, NETWORK_IND                              AS                                NETWORK_IND
			from LOGIC_NET
            )
 		
                 
----FILTER LAYER(uses aliases)----
,
        FILTER_IW as ( SELECT  * from     RENAME_IW WHERE IW_PRIMARY_SOURCE_SYSTEM != 'MANUAL ENTRY' ),
        FILTER_EMP as ( SELECT  * from     RENAME_EMP WHERE EMP_PRIMARY_SOURCE_SYSTEM != 'MANUAL ENTRY' ),
		FILTER_PROV as ( SELECT  *  from    RENAME_PROV WHERE PRIMARY_SOURCE_SYSTEM != 'MANUAL ENTRY' ),
        FILTER_NET as ( SELECT  * from     RENAME_NET WHERE PRIMARY_SOURCE_SYSTEM != 'MANUAL ENTRY' ),
        FILTER_REP as ( SELECT  * from     RENAME_REP WHERE REP_PRIMARY_SOURCE_SYSTEM != 'MANUAL ENTRY' )
----JOIN LAYER----
,
 JOIN_CUST as (  
 		SELECT 
 		     COALESCE(EMP_CUSTOMER_HKEY,REP_CUSTOMER_HKEY,IW_CUSTOMER_HKEY) AS CUSTOMER_HKEY
           , COALESCE(EMP_CUSTOMER_NUMBER,REP_CUSTOMER_NUMBER,IW_CUSTOMER_NUMBER) AS CUSTOMER_NUMBER
           , COALESCE(EMP_CUSTOMER_NAME,REP_CUSTOMER_NAME,IW_CUSTOMER_NAME) AS CUSTOMER_NAME
           , COALESCE(EMP_PHONE_NUMBER,REP_PHONE_NUMBER,IW_PHONE_NUMBER) AS PHONE_NUMBER
           , COALESCE(EMP_DOCUMENT_BLOCK_IND,REP_DOCUMENT_BLOCK_IND,IW_DOCUMENT_BLOCK_IND) AS DOCUMENT_BLOCK_IND
           , COALESCE(EMP_PHYSICAL_ADDRESS_LINE_1, REP_PHYSICAL_ADDRESS_LINE_1, IW_PHYSICAL_ADDRESS_LINE_1) AS PHYSICAL_ADDRESS_LINE_1
           , COALESCE(EMP_PHYSICAL_ADDRESS_LINE_2,REP_PHYSICAL_ADDRESS_LINE_2,IW_PHYSICAL_ADDRESS_LINE_2) AS PHYSICAL_ADDRESS_LINE_2
           , COALESCE(EMP_PHYSICAL_ADDRESS_CITY,REP_PHYSICAL_ADDRESS_CITY,IW_PHYSICAL_ADDRESS_CITY) AS PHYSICAL_ADDRESS_CITY
           , COALESCE(EMP_PHYSICAL_ADDRESS_STATE_CODE, REP_PHYSICAL_ADDRESS_STATE_CODE,IW_PHYSICAL_ADDRESS_STATE_CODE) AS PHYSICAL_ADDRESS_STATE_CODE
           , COALESCE(EMP_PHYSICAL_ADDRESS_POSTAL_CODE, REP_PHYSICAL_ADDRESS_POSTAL_CODE,IW_PHYSICAL_ADDRESS_POSTAL_CODE) AS PHYSICAL_ADDRESS_POSTAL_CODE
           , COALESCE(EMP_PHYSICAL_ADDRESS_COUNTY,REP_PHYSICAL_ADDRESS_COUNTY,IW_PHYSICAL_ADDRESS_COUNTY) AS PHYSICAL_ADDRESS_COUNTY
           , COALESCE(EMP_MAILING_ADDRESS_LINE_1, REP_MAILING_ADDRESS_LINE_1,IW_MAILING_ADDRESS_LINE_1) AS MAILING_ADDRESS_LINE_1
           , COALESCE(EMP_MAILING_ADDRESS_LINE_2, REP_MAILING_ADDRESS_LINE_2,IW_MAILING_ADDRESS_LINE_2) AS MAILING_ADDRESS_LINE_2
           , COALESCE(EMP_MAILING_ADDRESS_CITY, REP_MAILING_ADDRESS_CITY,IW_MAILING_ADDRESS_CITY) AS MAILING_ADDRESS_CITY
           , COALESCE(EMP_MAILING_ADDRESS_STATE_CODE, REP_MAILING_ADDRESS_STATE_CODE,IW_MAILING_ADDRESS_STATE_CODE) AS MAILING_ADDRESS_STATE_CODE
           , COALESCE(EMP_MAILING_ADDRESS_POSTAL_CODE, REP_MAILING_ADDRESS_POSTAL_CODE,IW_MAILING_ADDRESS_POSTAL_CODE) AS MAILING_ADDRESS_POSTAL_CODE
           , COALESCE(EMP_MAILING_ADDRESS_COUNTY_NAME, REP_MAILING_ADDRESS_COUNTY_NAME,IW_MAILING_ADDRESS_COUNTY_NAME) AS MAILING_ADDRESS_COUNTY_NAME
           , COALESCE(EMP_CURRENT_RECORD_IND, IW_CURRENT_RECORD_IND,REP_CURRENT_RECORD_IND) AS CURRENT_RECORD_IND
           , COALESCE(EMP_RECORD_EFFECTIVE_DATE, REP_RECORD_EFFECTIVE_DATE ,IW_RECORD_EFFECTIVE_DATE) AS RECORD_EFFECTIVE_DATE
           , COALESCE(EMP_RECORD_END_DATE, REP_RECORD_END_DATE ,IW_RECORD_END_DATE) AS RECORD_END_DATE
           , COALESCE(EMP_LOAD_DATETIME, REP_LOAD_DATETIME,IW_LOAD_DATETIME) AS LOAD_DATETIME
           , COALESCE(EMP_UPDATE_DATETIME, REP_UPDATE_DATETIME ,IW_UPDATE_DATETIME) AS UPDATE_DATETIME
           , COALESCE(EMP_PRIMARY_SOURCE_SYSTEM, REP_PRIMARY_SOURCE_SYSTEM ,IW_PRIMARY_SOURCE_SYSTEM) AS PRIMARY_SOURCE_SYSTEM
		   , MAX(EMPLOYER_IND) AS EMPLOYER_IND
		   , MAX(REPRESENTATIVE_IND) AS REPRESENTATIVE_IND
		   , MAX(INJURED_WORKER_IND) AS INJURED_WORKER_IND
		   
FROM  FILTER_EMP
FULL OUTER JOIN FILTER_REP ON FILTER_EMP.EMP_CUSTOMER_NUMBER = FILTER_REP.REP_CUSTOMER_NUMBER  AND FILTER_REP.REP_RECORD_EFFECTIVE_DATE BETWEEN FILTER_EMP.EMP_RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.EMP_RECORD_END_DATE,'2099-12-31')
FULL OUTER JOIN FILTER_IW ON FILTER_EMP.EMP_CUSTOMER_NUMBER = FILTER_IW.IW_CUSTOMER_NUMBER AND FILTER_IW.IW_RECORD_EFFECTIVE_DATE BETWEEN FILTER_EMP.EMP_RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.EMP_RECORD_END_DATE,'2099-12-31')
GROUP BY CUSTOMER_HKEY, CUSTOMER_NUMBER, CUSTOMER_NAME, PHONE_NUMBER, DOCUMENT_BLOCK_IND, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_STATE_CODE, PHYSICAL_ADDRESS_POSTAL_CODE
, PHYSICAL_ADDRESS_COUNTY, MAILING_ADDRESS_LINE_1, MAILING_ADDRESS_LINE_2, MAILING_ADDRESS_CITY, MAILING_ADDRESS_STATE_CODE, MAILING_ADDRESS_POSTAL_CODE, MAILING_ADDRESS_COUNTY_NAME, CURRENT_RECORD_IND
, RECORD_EFFECTIVE_DATE, RECORD_END_DATE, LOAD_DATETIME, UPDATE_DATETIME, PRIMARY_SOURCE_SYSTEM
 )

----ETL LAYER----

select
  CUSTOMER_HKEY
, CUSTOMER_NUMBER::VARCHAR() AS CUSTOMER_NUMBER
, CUSTOMER_NAME
, PHONE_NUMBER
, DOCUMENT_BLOCK_IND
, PHYSICAL_ADDRESS_LINE_1
, PHYSICAL_ADDRESS_LINE_2
, PHYSICAL_ADDRESS_CITY
, PHYSICAL_ADDRESS_STATE_CODE
, PHYSICAL_ADDRESS_POSTAL_CODE
, PHYSICAL_ADDRESS_COUNTY
, MAILING_ADDRESS_LINE_1
, MAILING_ADDRESS_LINE_2
, MAILING_ADDRESS_CITY
, MAILING_ADDRESS_STATE_CODE
, MAILING_ADDRESS_POSTAL_CODE
, MAILING_ADDRESS_COUNTY_NAME
, EMPLOYER_IND
, REPRESENTATIVE_IND
, INJURED_WORKER_IND
, NULL AS PROVIDER_IND
, NULL AS NETWORK_IND
, CURRENT_RECORD_IND
, RECORD_EFFECTIVE_DATE
, RECORD_END_DATE
, LOAD_DATETIME
, UPDATE_DATETIME
, PRIMARY_SOURCE_SYSTEM
from JOIN_CUST 

UNION


SELECT 
 		     CUSTOMER_HKEY
           , CUSTOMER_NUMBER::VARCHAR() AS CUSTOMER_NUMBER
           , CUSTOMER_NAME
           , PHONE_NUMBER::VARCHAR(12) AS PHONE_NUMBER
           , DOCUMENT_BLOCK_IND
           , PHYSICAL_ADDRESS_LINE_1
           , PHYSICAL_ADDRESS_LINE_2
           , PHYSICAL_ADDRESS_CITY
           , PHYSICAL_ADDRESS_STATE_CODE
           , PHYSICAL_ADDRESS_POSTAL_CODE
           , PHYSICAL_ADDRESS_COUNTY
           , MAILING_ADDRESS_LINE_1
           , MAILING_ADDRESS_LINE_2
           , MAILING_ADDRESS_CITY
           , MAILING_ADDRESS_STATE_CODE
           , MAILING_ADDRESS_POSTAL_CODE
           , MAILING_ADDRESS_COUNTY_NAME
		   , NULL AS EMPLOYER_IND
		   , NULL AS REPRESENTATIVE_IND
		   , NULL AS INJURED_WORKER_IND
           , PROVIDER_IND
           , NULL AS NETWORK_IND
           , CURRENT_RECORD_IND
           , RECORD_EFFECTIVE_DATE
           , RECORD_END_DATE
           , LOAD_DATETIME
           , UPDATE_DATETIME
           , PRIMARY_SOURCE_SYSTEM
FROM  FILTER_PROV
UNION
		SELECT 
 		     CUSTOMER_HKEY
           , CUSTOMER_NUMBER::VARCHAR() AS CUSTOMER_NUMBER
           , CUSTOMER_NAME
           , PHONE_NUMBER::VARCHAR(12) AS PHONE_NUMBER
           , DOCUMENT_BLOCK_IND
           , PHYSICAL_ADDRESS_LINE_1
           , PHYSICAL_ADDRESS_LINE_2
           , PHYSICAL_ADDRESS_CITY
           , PHYSICAL_ADDRESS_STATE_CODE
           , PHYSICAL_ADDRESS_POSTAL_CODE
           , PHYSICAL_ADDRESS_COUNTY
           , MAILING_ADDRESS_LINE_1
           , MAILING_ADDRESS_LINE_2
           , MAILING_ADDRESS_CITY
           , MAILING_ADDRESS_STATE_CODE
           , MAILING_ADDRESS_POSTAL_CODE
           , MAILING_ADDRESS_COUNTY_NAME
		   , NULL AS EMPLOYER_IND
		   , NULL AS REPRESENTATIVE_IND
		   , NULL AS INJURED_WORKER_IND	
           , NULL AS PROVIDER_IND	   
           , NETWORK_IND
           , CURRENT_RECORD_IND
           , RECORD_EFFECTIVE_DATE
           , RECORD_END_DATE
           , LOAD_DATETIME
           , UPDATE_DATETIME
           , PRIMARY_SOURCE_SYSTEM
FROM  FILTER_NET