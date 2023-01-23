

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(USER_LOGIN_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_LOGIN_NAME, 
     last_value(USER_LAST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_LAST_NAME, 
     last_value(USER_FIRST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_FIRST_NAME, 
     last_value(USER_MIDDLE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_MIDDLE_NAME,
     last_value(USER_FULL_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_FULL_NAME,        
     last_value(USER_TYPE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_TYPE, 
     last_value(USER_ROLE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_ROLE, 
     last_value(USER_EMAIL) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_EMAIL, 
     last_value(USER_PHONE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_PHONE_NUMBER, 
     last_value(USER_PHONE_NUMBER_EXTENSION) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as USER_PHONE_NUMBER_EXTENSION, 
     last_value(SUPERVISOR_LAST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUPERVISOR_LAST_NAME, 
     last_value(SUPERVISOR_FIRST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUPERVISOR_FIRST_NAME, 
     last_value(SUPERVISOR_MIDDLE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUPERVISOR_MIDDLE_NAME,
      last_value(SUPERVISOR_FULL_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUPERVISOR_FULL_NAME,
       last_value(DIRECTOR_LAST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIRECTOR_LAST_NAME,
       last_value(DIRECTOR_FIRST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIRECTOR_FIRST_NAME,                 
       last_value(DIRECTOR_MIDDLE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIRECTOR_MIDDLE_NAME, 
        last_value(DIRECTOR_FULL_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIRECTOR_FULL_NAME       
	FROM EDW_STAGING.DIM_USER_SCDALL_STEP2),

-- ETL Layer---

ETL AS (
SELECT
 UNIQUE_ID_KEY AS USER_HKEY
,UNIQUE_ID_KEY
,USER_LOGIN_NAME
,USER_LAST_NAME
,USER_FIRST_NAME
,USER_MIDDLE_NAME
,USER_FULL_NAME
,USER_TYPE
,USER_ROLE
,USER_EMAIL
,USER_PHONE_NUMBER
,USER_PHONE_NUMBER_EXTENSION
,SUPERVISOR_LAST_NAME
,SUPERVISOR_FIRST_NAME
,SUPERVISOR_MIDDLE_NAME
,SUPERVISOR_FULL_NAME
,DIRECTOR_LAST_NAME
,DIRECTOR_FIRST_NAME
,DIRECTOR_MIDDLE_NAME
,DIRECTOR_FULL_NAME
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE'  AS PRIMARY_SOURCE_SYSTEM
from  SCD
)
select * from ETL