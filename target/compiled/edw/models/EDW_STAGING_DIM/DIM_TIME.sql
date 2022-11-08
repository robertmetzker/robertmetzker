

---- SRC LAYER ----
WITH
SRC_TME as ( SELECT *     from     STAGING.DSV_TIME ),
//SRC_TME as ( SELECT *     from     DSV_TIME) ,

---- LOGIC LAYER ----


LOGIC_TME as ( SELECT 
		  TIME_KEY                                           as                                           TIME_KEY 
		, TRIM( TIME_HHMMSS )                                as                                        TIME_HHMMSS 
		, TRIM( TIME_HHMM )                                  as                                          TIME_HHMM 
		, TRIM( TIME_HH )                                    as                                            TIME_HH 
		, TRIM( TIME_HOUR )                                  as                                          TIME_HOUR 
		, TIME_HOUR_SORT                                     as                                     TIME_HOUR_SORT 
		, TRIM( TIME_MINUTE )                                as                                        TIME_MINUTE 
		, TIME_MINUTE_SORT                                   as                                   TIME_MINUTE_SORT 
		, TRIM( TIME_SECOND )                                as                                        TIME_SECOND 
		, TIME_SECOND_SORT                                   as                                   TIME_SECOND_SORT 
		, TRIM( TIME_HALFHOUR )                              as                                      TIME_HALFHOUR 
		, TIME_HALFHOUR_SORT                                 as                                 TIME_HALFHOUR_SORT 
		from SRC_TME
            )

---- RENAME LAYER ----
,

RENAME_TME as ( SELECT 
		  TIME_KEY                                           as                                           TIME_KEY
		, TIME_HHMMSS                                        as                                        TIME_HHMMSS
		, TIME_HHMM                                          as                                          TIME_HHMM
		, TIME_HH                                            as                                            TIME_HH
		, TIME_HOUR                                          as                                          TIME_HOUR
		, TIME_HOUR_SORT                                     as                                     TIME_HOUR_SORT
		, TIME_MINUTE                                        as                                        TIME_MINUTE
		, TIME_MINUTE_SORT                                   as                                   TIME_MINUTE_SORT
		, TIME_SECOND                                        as                                        TIME_SECOND
		, TIME_SECOND_SORT                                   as                                   TIME_SECOND_SORT
		, TIME_HALFHOUR                                      as                                      TIME_HALFHOUR
		, TIME_HALFHOUR_SORT                                 as                                 TIME_HALFHOUR_SORT 
				FROM     LOGIC_TME   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TME                            as ( SELECT * from    RENAME_TME   ),

---- JOIN LAYER ----

 JOIN_TME  as  ( SELECT * 
				FROM  FILTER_TME )
select 
TIME_KEY,
TIME_HHMMSS,
TIME_HHMM,
TIME_HH,
TIME_HOUR,
TIME_HOUR_SORT,
TIME_MINUTE,
TIME_MINUTE_SORT,
TIME_SECOND,
TIME_SECOND_SORT,
TIME_HALFHOUR,
TIME_HALFHOUR_SORT,
CURRENT_TIMESTAMP AS  LOAD_DATETIME,
TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME,
'DW_REPORT' AS PRIMARY_SOURCE_SYSTEM 
from JOIN_TME
--where time_minute = time_halfhour and right(time_second_sort,2) = '00'
ORDER BY TIME_KEY