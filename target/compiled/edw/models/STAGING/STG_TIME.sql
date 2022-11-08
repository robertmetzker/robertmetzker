with wh_time_of_day as( select seq4() as rn
    , '1900-01-01T00:00:00'::timestamp as tstamp
//    tstamp + interval '1 second'
    , dateadd(second,rn,tstamp) as time_of_day
from table(generator(rowcount => 86400)) ),
kg as (
SELECT
TO_CHAR(TIME_OF_DAY, 'HH24MISS'):: INT AS TIME_KEY,
TO_CHAR(time_of_day, 'HH:MI:SS') AS time_hhmmss,
TO_CHAR(time_of_day, 'HH:MI') AS time_hhmm,
TO_CHAR(time_of_day, 'HH') AS time_hh,
-- 12 hour clock time in hours
ltrim( TO_CHAR( time_of_day, 'HH12 AM'),'0') as time_hour,
EXTRACT(HOUR FROM time_of_day) AS time_hour_sort, -- integer for hour sort
-- 12 hour clock down to minutes
ltrim( TO_CHAR( time_of_day, 'HH12:MI AM'),'0') as time_minute,
TO_CHAR(TIME_OF_DAY, 'HH24MI'):: INT    AS time_minute_sort,  -- integer HHMM for sorting
-- 12 hour clock down to seconds
ltrim( TO_CHAR( time_of_day, 'HH12:MI:SS AM'),'0') as time_second,
TO_CHAR(TIME_OF_DAY, 'HH24MISS'):: int AS time_second_sort,  -- integer HHMMSS for sorting
-- half hour intervals (similar for quarter hour intervals)
ltrim( TO_CHAR( time_slice(time_of_day, 30, 'MINUTE'), 'HH12:MI AM'),'0') as time_halfhour,
-- half hour interval sort (integer formatted as HH00 or HH30)
TO_CHAR( time_slice(time_of_day, 30, 'MINUTE'), 'HH24MI'):: int as time_halfhour_sort
  from wh_time_of_day
)
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
TIME_HALFHOUR_SORT
from kg
--where time_minute = time_halfhour and right(time_second_sort,2) = '00'
ORDER BY TIME_KEY