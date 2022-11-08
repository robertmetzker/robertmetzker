

---- SRC LAYER ----
WITH
SRC_PS as ( SELECT *     from     STAGING.DST_POLICY_STANDING ),
//SRC_PS as ( SELECT *     from     DST_POLICY_STANDING) ,

---- LOGIC LAYER ----

LOGIC_PS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PLCY_TYP_CODE                                      as                                   POLICY_TYPE_CODE 
		, PLCY_TYP_NAME                                      as                                   POLICY_TYPE_DESC 
		, PLCY_STS_TYP_CD                                    as                                 POLICY_STATUS_CODE 
		, PLCY_STS_TYP_NM                                    as                                 POLICY_STATUS_DESC 
		, PLCY_STS_RSN_TYP_CD                                as                                 STATUS_REASON_CODE 
		, PLCY_STS_RSN_TYP_NM                                as                                 STATUS_REASON_DESC 
		, ACT_IND                                            as                                  POLICY_ACTIVE_IND 
		from SRC_PS
            )

---- RENAME LAYER ----
,

RENAME_PS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, POLICY_STATUS_CODE                                 as                                 POLICY_STATUS_CODE
		, POLICY_STATUS_DESC                                 as                                 POLICY_STATUS_DESC
		, STATUS_REASON_CODE                                 as                                 STATUS_REASON_CODE
		, STATUS_REASON_DESC                                 as                                 STATUS_REASON_DESC
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
				FROM     LOGIC_PS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PS                             as ( SELECT * from    RENAME_PS   ),

---- JOIN LAYER ----

 JOIN_PS  as  ( SELECT * 
				FROM  FILTER_PS )
 SELECT * FROM  JOIN_PS