#std libs
import os, csv, types
from pathlib import Path
#other libs
import openpyxl
#-------------------- infrastructure
def read_csv( fname, delim=',' ):
    rows=[]
    for idx,row in enumerate( csv.DictReader( open( fname ), delimiter=delim )):
            rows.append( row )
    return rows

def sht2dict( sheet ):
    fields = None
    rows = []
    for idx,row in enumerate( sheet.iter_rows( values_only = True )):
        if idx == 0:
            fields = row
            continue
        row=dict(zip( fields, row ))
        rows.append( row )
    return rows

def load_excel( fname, sheet_name):
    '''
    {'target': 'network_hist', 'src': 'DEV_EDW.STAGING.STG_NETWORK', 'id': 'NTWK_NUMBER', 'cols': 'NTWK_NAME,NTWK_STATUS_IDN,NTWK_STATUS_DSC', 'start': 'START_DATE', 'end': 'END_DATE'}
    tables = [
         {
            'target':'network_hist',
            'src':'DEV_EDW.STAGING.STG_NETWORK',
            'id':['NTWK_NUMBER'],
            'cols':['NTWK_NAME','NTWK_STATUS_IDN','NTWK_STATUS_DSC'],
            'start':'START_DATE',
            'end':'END_DATE',    
            'where':'',
            'aliases':'',
        },    

    '''
    before =[]
    after = []
    tables = {}
    
    wb = openpyxl.load_workbook( fname )
    for row in sht2dict(wb[ sheet_name ]):
        target = row[ 'target' ]
        if target not in tables:
            tables[ target ] = []
        row_dict={
                'target': row[ 'target' ].strip(),
                'src': row[ 'src' ].strip(),
                'id': [ theid.strip() for theid in row[ 'unique_id_key' ].split(',') ],
                'cols': [ col.strip() for col in row[ 'cols_scd2_scd0' ].split(',') ],
                'start': row[ 'start' ].strip(),
                'end': row[ 'end' ].strip(),
                'where': row.get( 'where', '' ),
                'aliases': row.get( 'aliases', ''),
        }

        # Loop through aliases and append them as key:value pairs for each colname:alias.
        if row_dict['aliases']:
            renamecols = [ thecol.strip() for thecol in row_dict['aliases'].split(',') ]
            target = row_dict.get('target','')
            for idx, alias in enumerate( renamecols):
                if ' AS ' in alias.upper():
                    compound = alias.upper().split(' AS ')
                    before.append( f'{target}_{compound[0].strip()}' )
                    after.append( compound[1].strip() )
                else:
                    before.append( alias )
                    after.append( alias )
            colaliases = {before[i]: after[i] for i in range(len(before)) }
            row_dict.update( colaliases )

        tables[ target ].append( row_dict )

    return tables


def load_csv( fname ):
    '''
    {'target': 'network_hist', 'src': 'DEV_EDW.STAGING.STG_NETWORK', 'id': 'NTWK_NUMBER', 'cols': 'NTWK_NAME,NTWK_STATUS_IDN,NTWK_STATUS_DSC', 'start': 'START_DATE', 'end': 'END_DATE'}
    tables = [
         {
            'target':'network_hist',
            'src':'DEV_EDW.STAGING.STG_NETWORK',
            'id':['NTWK_NUMBER'],
            'cols':['NTWK_NAME','NTWK_STATUS_IDN','NTWK_STATUS_DSC'],
            'start':'START_DATE',
            'end':'END_DATE',    
            'where':'',
            'aliases': row.get( 'aliases', ''),

        },    

    '''

    tables = {}

    for row in read_csv( fname ):
        target = row[ 'target' ]
        if target not in tables:
            tables[ target ] = []
        row_dict={
                'target': row[ 'target' ].strip(),
                'src': row[ 'src' ].strip(),
                'id': [ theid.strip() for theid in row[ 'unique_id_key' ].split(',') ],
                'cols': [ col.strip() for col in row[ 'cols_scd2_scd0' ].split(',') ],
                'start': row[ 'start' ].strip(),
                'end': row[ 'end' ].strip(),
                'where': row.get( 'where', '' ),
                'aliases': row.get( 'aliases', ''),
        }
        tables[ target ].append( row_dict )

    return tables


#------------------------
def get_common_hist( args, tables ):
    '''
    unpivot the dates for all of the tables to make a common history table
    '''

    union_sql = []
    for idx, tbl in enumerate( tables ):
        sql = f'''select UNIQUE_ID_KEY, adate from tbl{idx} unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) '''
        union_sql.append( sql )

    union_str = '\n\t\tunion\n\t'.join( union_sql )
    boundary = '-- ' + '*'*80 +f'\n'
    sql = f'''
{ boundary }
-----------------------------(get_common_hist) FINAL DATES TABLE union all dates, join to get values
--	unpivot dbt_valid_from, dbt_valid_to  into a single column adate, all other columns are gone
, combo_dates as (
    { union_str }
)

--- remove duplicate dates with distinct
, unique_dates as (
    select distinct * from combo_dates order by adate    
)
--- convert single column back into 2 columns aka pivot
, final_dates as (
    select UNIQUE_ID_KEY, 
        adate as dbt_valid_from, 
        lead( adate, 1 ) over ( partition by  UNIQUE_ID_KEY order by adate ) dbt_valid_to 
from unique_dates  
)
    '''

    return sql

    '''
        //,add_rn as (
        //    select *,
        //        row_number() OVER(partition by ID order by dbt_valid_from,dbt_valid_to) as rn
        //    from add_vals
        //    
        //)
        //,add_aday as (
        //    select *,
        //        IFF(rn>1, DATEADD(day,1,dbt_valid_from),dbt_valid_from) as dbt_valid_from_add
        //        
        //    from add_rn
        //    
        //)
    '''


    return sql

def make_cols( args, mapping, target ):
    '''
    Rename columns by adding the table in front, this ensures all columns are unique.
    '''
    prefix = mapping[ 'src' ].split('.')[-1]
    new_cols = []
    for idx, col in enumerate(sorted( mapping[ 'cols' ])):
        aliascol = f"{target}_{col}"
        if not mapping.get(aliascol,'') == '':
            aliasnm = mapping[aliascol]
            new_cols.append(f'\n\t\t{col} as {prefix}_{aliasnm}')
        else:
            new_cols.append(f'\n\t\t{col} as {prefix}_{col}')
    
    col_str = ', '.join(new_cols)

    return col_str+'\n'


def make_final_cols( args, mapping ):
    prefix = mapping[ 'src' ].split('.')[ -1 ]
    new_cols = []
    
    for idx, col in enumerate(sorted( mapping[ 'cols' ])):
        new_cols.append(f'\n\t\t{col} as {prefix}_{col}')
    
    col_str = ', '.join( new_cols )

    return col_str+'\n'

def map_2_final( args, tables ):
    '''
    rename unique cols back to original names
    add dbt_sdc_id

    '''

    table_cols = []

    for idx, tbl in enumerate( tables ):
        prefix = tbl[ 'src' ].split('.')[ -1 ]
        target = tbl.get('target','')
        if len(tbl) > 9:
            for idx, col in enumerate(sorted( tbl[ 'cols' ])):
                aliascol = f'{target}_{col}'
                aliasnm = tbl.get(aliascol,'')
                if not aliasnm == '':
                    table_cols.append(f'\n\t\t{prefix}_{aliasnm} as {aliasnm} ')
                else:
                    table_cols.append(f'\n\t\t{prefix}_{col} as {col} ')
        else: 
            for idx, col in enumerate(sorted( tbl[ 'cols' ])):
                table_cols.append(f'\n\t\t{prefix}_{col} as {col} ')
        
    col_str = ', '.join( table_cols )

    rename_sql = f'''
        ------------(map_2_final) name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '{args.dbt_delim}' || coalesce(cast( CURRENT_DATE::timestamp as varchar ), '')) as dbt_scd_id,
                    --data cols
                    {col_str},

                    to_timestamp_ntz( CURRENT_DATE::timestamp ) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF( dbt_valid_to, TO_DATE('12/31/2999') ) as dbt_valid_to
                    from join_sql
            )
    '''


    return rename_sql

def add_fix_granularity_sql(args, tbl, tblidx, start, end, theid, start_suffix = '_initial' ):
    '''
    namespace( debug = True ) {'src': 'DEV_EDW.STAGING.STG_TDDIDCS', 'id': "md5(coalesce(cast( ICDC_CODE as varchar ), '')|| '|' || coalesce(cast( ICDV_CODE as varchar ), ''))", 'cols': [ 'CMS_ICD_STS_CODE', 'CMS_ICD_STS' ], 
        'start': 'to_timestamp_ntz( IDCS_EFCTV_DATE )', 'end': 'to_timestamp_ntz( IDCS_ENDNG_DATE )', 'where': 'WHERE IDCS_VRSN_END_DATE > CURRENT_DATE()', 
        'debugid': "coalesce(cast( ICDC_CODE as varchar ), '')|| '|' || coalesce(cast( ICDV_CODE as varchar ),'')"} tbl0 dbt_valid_from dbt_valid_to UNIQUE_ID_KEY
    '''

    prefix = ''
    if '.' in tbl[ 'src' ]: 
        prefix = tbl[ 'src' ].split('.')[ -1 ]+'_'
    elif tblidx == 'rename':
        prefix =''
    else: 
        prefix = tbl[ 'src' ]+'_'   
        if prefix == 'RENAME':
            pass

    boundary = '-- ' + '*'*80 +f'\n'
    cols = []
    nvl = args.nullval
    target = tbl.get('target','')

    #coalesce(cast(ICDV_CODE as varchar ), '')
    if len(tbl) > 9:
        for col in tbl['cols']:
            aliascol = f'{target}_{col}'
            aliasnm = tbl.get(aliascol,'')
            # col = tbl.get(col,'')
            if not aliasnm == '':
                # cols = [f"coalesce(cast( {prefix}{aliasnm} as varchar ), '{args.nullval}' )" for col in tbl[ 'cols' ]]
                cols += [ f"coalesce(cast( {prefix}{aliasnm} as varchar ), '{nvl}' )" ]
            else:
                cols += [ f"coalesce(cast( {prefix}{col} as varchar ), '{nvl}' )" ]

        ids = [ args.unique_id_key ] + cols
    else:
        cols = [f"coalesce(cast( {prefix}{col} as varchar ), '{args.nullval}' )" for col in tbl[ 'cols' ]]
        ids = [ args.unique_id_key ] + cols

    collapse_id = f"\n\t\t\t|| '{args.collapse_delim}' ||".join( ids )
        
    #select split_part(collapse_id,'|',1) as id,split_part(collapse_id,'|',2) as data1,*

    split_sql = f'''nullif(split_part( {tblidx}_collapse_id, '~',1 ),'{args.nullval}' ) as {args.unique_id_key}, '''

    target = tbl.get('target','')

    if len(tbl) > 9:
        for idx, col in enumerate(tbl[ 'cols' ]):
            aliascol = f'{target}_{col}'
            aliasnm = tbl.get(aliascol,'')
            col = tbl.get(col,'')
            if not aliasnm == '':
                split_sql += f'''nullif(split_part( {tblidx}_collapse_id, '~', {idx+2}), '{args.nullval}' ) as {prefix}{aliasnm}, '''
            else:
                split_sql += f'''nullif(split_part( {tblidx}_collapse_id, '~', {idx+2}), '{args.nullval}' ) as {prefix}{col}, '''

    else:
        for idx, col in enumerate(tbl[ 'cols' ]):
            split_sql += f'''nullif(split_part( {tblidx}_collapse_id, '~', {idx+2}), '{args.nullval}' ) as {prefix}{col}, '''

    split_sql = split_sql.replace('nullif(', '\n\t\tnullif(')

    sql=f'''\n\n{boundary}-----------------------------(add_fix_granularity_sql) creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , {tblidx}_build_granularity as (
        select *, 
            min( {collapse_id} ) OVER( PARTITION BY {collapse_id}) as {tblidx}_collapse_id
    from {tblidx}
    )

    , {tblidx}_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY {tblidx}_collapse_id ORDER BY {start}{start_suffix}, {end}) AS RN,
        LAG( {end}, 1 ) OVER ( PARTITION BY {tblidx}_collapse_id ORDER BY {start}{start_suffix}, {end}) AS PEND
    FROM
         {tblidx}_build_granularity
    )
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , {tblidx}_add_gapid as (
    select 
            *,
            case when {start}{start_suffix} > dateadd( day, 1, {tblidx}_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY {tblidx}_collapse_id ORDER BY {tblidx}_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from {tblidx}_add_lag )
    --- collapse every row with same gapid into one row 
    , {tblidx}_collapse as 

        ( 
        select 
        {tblidx}_collapse_id, min( {start}{start_suffix} ) as {start}{start_suffix}, max( {end} ) as {end}
        from {tblidx}_add_gapid
        group by {tblidx}_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , {tblidx}_granularity as (
        select     {split_sql} 
        *
        from {tblidx}_collapse
    )


'''

    return sql


def add_line_up_the_dates_sql(args, table, start, end, id, start_suffix = '_initial' ):
    sql=f'''
----------------------(add_line_up_the_dates_sql) filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, {table}_add_lag2 as (
SELECT
    *,
    LAG( {end}, 1 ) OVER( PARTITION BY {args.unique_id_key} ORDER BY {start}{start_suffix}, {end} ) AS PREV_{end},
    ROW_NUMBER() OVER( PARTITION BY {args.unique_id_key} ORDER BY {start}{start_suffix}, {end} ) AS RN --  for smushing
    FROM
        {table}_setup_granularity
order by {args.unique_id_key}, {start}{start_suffix}
)
--- if jan6( next row initial ) > jan3( previous_to )
, {table}_mark_gap2 as ( 
select 
        *,
--        case when  {start}{start_suffix} > DATEADD( 'DAY', 1, PREV_{end} )  then 1 else 0 END as gap,
--        case when  {start}{start_suffix} =  PREV_{end}  then DATEADD( 'DAY', 1, {start}{start_suffix} ) else {start}{start_suffix} END as {start}_2a -- making 
        case when  {start}{start_suffix} >  PREV_{end}   then 1 else 0 END as gap,
        case when  {start}{start_suffix} =  PREV_{end}  then {start}{start_suffix}  else {start}{start_suffix} END as {start}_2a -- placeholder for dateadd 
from {table}_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, {table}_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_{end} ) else  {start}_2a  end as {start}_2b  -- made up history!!!
    case when gap = 1 then  PREV_{end} else  {start}_2a  end as {start}_2b  -- made up history!!!
    from {table}_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, {table} as (
--    select  {args.unique_id_key}, {start}_2B as  {start}, {end} from  fill_gap
select *, {start}_2B as  {start} from  {table}_fill_gap
)

    '''

    return sql


def clean_multidate( col, using ):
    '''
    NOTE: the flattens the multiple dates using LEAST or GREATEST
    this fixes invalid dates like 10000 and converts them to 2999-12-31

    nullif( least( coalesce( MAIL_ADRS_EFF_DATE, '2999-12-31 00:00:00'), coalesce( PHYS_ADRS_EFF_DATE, '2999-12-31 00:00:00')),'2999-12-31 00:00:00') 
         as dbt_valid_from_initial_1,
    nullif( greatest( coalesce( MAIL_ADRS_END_DATE, '1901-12-31 00:00:00'), coalesce( PHYS_ADRS_END_DATE, '1901-12-31 00:00:00')),'1901-12-31 00:00:00') 
         as dbt_valid_to_1
    '''
    startend = col.split(',')

    if using == 'from':
        sql=f'''
        nullif( LEAST( coalesce( {startend[0]}, '2999-12-31 00:00:00'), coalesce({startend[-1]}, '2999-12-31 00:00:00')), '2999-12-31 00:00:00') 
            '''
    else:
        sql=f'''
        nullif( GREATEST( coalesce( {startend[0]}, '2999-12-31 00:00:00'), coalesce({startend[-1]}, '2999-12-31 00:00:00')), '2999-12-31 00:00:00') 
            '''
    return sql


def clean_date( col ):
    '''
    NOTE: valid future dates will break!
    this fixes invalid dates like 10000 and converts them to 2999-12-31

    IFF( IFNULL(try_to_timestamp( DCTVT_DTTM ), TO_DATE( '12/31/2999' )) > CURRENT_DATE, TO_DATE( '12/31/2999' ), DCTVT_DTTM )
    '''

    sql=f'''
        IFF( IFNULL( try_to_timestamp( {col}::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), {col} )
        '''

    return sql



def map_2_generic_tables( args, tables, start, end, theid ):
    '''
    Convert specific table to generic table to match the structure the code expects.
    Remove extra rows and ensure start/end dates line up

    ex: UNIQUE_ID_KEY is the column that the code downstream expects to exist. So the actual unique col needs to be renamed.
    Also rename columns to be sure there are no clashes across tables for other columns.
    clean up the dates so they do not break like 10000
    '''

    table_sqls = []
    keep_list = ['cols', 'target', 'src', 'id', 'start', 'end', 'where', 'aliases', 'debugid' ]

    for idx, tbl in enumerate( tables ):
        target = tbl.get('target','')
        # If the table source changed, reset the column aliases
        # for i in list( tbl ):
        #     if i not in keep_list:
        #         del tbl[i]

        if not tbl['where']:
            whereclause = ''
        else:
            whereclause = tbl.get( 'where', "" )
            if not 'where' in whereclause.lower(): whereclause = 'WHERE  ' + whereclause
        theid = tbl[ 'id' ]
        if args.debug:
            theid = tbl[ 'debugid' ]
        tblidx = f'tbl{idx}'
        
        # Set column and aliascolumns and decide which to use based on idx
        cols = make_cols( args, tbl, target ) # give col unique names

        if ',' in  tbl[ 'start' ]: 
            startend = cols.split(',')
            vldfrom = clean_multidate( tbl[ 'start' ], 'from')
            vldto = clean_multidate( tbl[ 'end' ], 'to')
        else:
            vldfrom = clean_date( tbl[ 'start' ] )
            vldto = clean_date( tbl[ 'end' ])
        boundary = '-- ' + '*'*80 +f'\n'
        table_sql = f'''{boundary}-- Setting up generic table {tblidx}_setup  via (map_2_generic_tables)
-- rename to generic col names and select cols for history
-- ********************************************************************************
{tblidx}_setup1 as (
select distinct \n\t\t{theid} as UNIQUE_ID_KEY,
    --data cols
    \t{cols},
    --start end cols
    { vldfrom }  as dbt_valid_from_initial_1,
    { vldto }  as dbt_valid_to_1 \n
    from {tbl[ 'src' ]}
    {whereclause}
        )
---- only select latest values for a day
, {tblidx}_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS {tblidx}_ROWN 
    from     {tblidx}_setup1 
            qualify {tblidx}_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, {tblidx}_setup as (
    select *, 
            TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     {tblidx}_setup2 
        )        
        '''
        #generic code below 

        #shrink to rows that only reresent changes that want to be tracks
        gran_sql = add_fix_granularity_sql( args, tbl, tblidx + '_setup', start, end, 'UNIQUE_ID_KEY' )
        #connect start and end dates (dropping rows would leave gaps)
        dates_sql = add_line_up_the_dates_sql( args, tblidx, start, end, theid )
        table_sql += gran_sql + dates_sql

        table_sqls.append( table_sql )

    sql = ',\n'.join( table_sqls )

    return sql



def get_join_sql( args, tables ):
    '''
    This joins the tables together to the common history table
    tbl0 is the driver
    inequality joins are used to handle date rangers
    '''

    #make select
    select = '\n----------------------(get_join_sql)  JOIN ALL tables together with matching dates\n'
    select += ', join_sql as ( \nselect \n\ttbl0.UNIQUE_ID_KEY,\n\t'

    #---- get generic col names
    cols = []

    for idx1, table in enumerate( tables ):
        target = table.get('target','')
        prefix = table[ 'src' ].split('.')[ -1 ]        
        for idx2, col in enumerate(sorted(table[ 'cols' ])):
            aliascol = f'{target}_{col}'
            aliasnm = table.get(aliascol,'')
            if not aliasnm == '':
                cols.append(f'tbl{idx1}.{prefix}_{aliasnm}')        
            else:
                cols.append(f'tbl{idx1}.{prefix}_{col}')        

            
    select += '\n\t, '.join( cols ) + ', '

    #join
    std_sql = '''\n\tfinal_dates.dbt_valid_from, final_dates.dbt_valid_to
    from tbl0
        inner join final_dates
        on tbl0.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and tbl0.dbt_valid_from < final_dates.dbt_valid_to and tbl0.dbt_valid_to > final_dates.dbt_valid_from
            '''
    left_sql = ''
    for i in range(1, len( tables )):
        left_sql += f'''\n\tleft join tbl{i} on  tbl{i}.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl{i}.dbt_valid_from < final_dates.dbt_valid_to  and  tbl{i}.dbt_valid_to > final_dates.dbt_valid_from'''


    cte_sql = select + std_sql + left_sql + ')'
    return cte_sql


def final_collapse( args, tgt, tables, theid ):
    '''
    after joining all of the tables if there are any duplicate rows with values, remove them
    '''

    theid = tables[ 0 ][ 'id' ]
    cols = []
    for table in tables:
        target = table.get('target','')
        if len(table) > 9:
            for idx, col in enumerate(table[ 'cols' ]):
                aliascol = f'{target}_{col}'
                aliasnm = table.get(aliascol,'')
                if not aliasnm == '':
                    cols += [ aliasnm ]
                else:
                    cols += [ col ]
        else:
            cols += table[ 'cols' ]

    begin = 'DBT_VALID_FROM'; end = 'DBT_VALID_TO'
    tbl = {}
    tbl[ 'src' ] = 'rename'
    tbl[ 'cols' ] = cols
    sql = add_fix_granularity_sql( args, tbl, 'rename', begin, end, theid, start_suffix = '').upper()

    return sql
#-------------------------------------------------------------------------

def merge_hist( args, tgt, tables ):
    start = 'dbt_valid_from'; end = 'dbt_valid_to'; theid = 'UNIQUE_ID_KEY'


    generic_tables = map_2_generic_tables( args, tables, start, end, theid )  #make sure incoming tables have right rows and structure
    common_hist_sql = get_common_hist( args, tables )  #make a "history table" that is an unpviot of all of the dates
    join_sql = get_join_sql( args, tables )  #join the tables together
    rename_sql = map_2_final( args, tables ) #add dbt_scd_id, rename cols back to original names
    final_collapse_sql = final_collapse( args, tgt, tables, theid )  #remove any joined duplicates

    #final sql
    cols = [
        "md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_DATE::timestamp as varchar ), '')) as dbt_scd_id",
        "to_timestamp( DBT_VALID_FROM ) as DBT_VALID_FROM",
        "to_timestamp( case when  ( row_number() over ( partition by UNIQUE_ID_KEY order by UNIQUE_ID_KEY, DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 THEN NULL else  DBT_VALID_TO end )  AS DBT_VALID_TO",
        'to_timestamp( CURRENT_DATE::timestamp ) as DBT_UPDATED_AT',
        ]

# ,CASE WHEN ( row_number() over ( partition by UNIQUE_ID_KEY order by DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 THEN NULL ELSE DBT_VALID_TO END AS DBT_VALID_TO

    if args.debug:
        cols = ['md5( UNIQUE_ID_KEY ) as UNIQUE_ID_KEY','UNIQUE_ID_KEY as DEBUG_ID' ] + cols
    else:
        cols = ['UNIQUE_ID_KEY'] + cols

    for table in tables:
        target = table.get('target','')
        if len(table) > 9:
            for col in table['cols']:
                aliascol = f'{target}_{col}'
                cols += [table.get(aliascol,'')]
                # cols += [table.get(col,'')]
            joincols = '\n\t, '.join( cols )
        else:
            cols += table[ 'cols' ]
            joincols = '\n\t, '.join( cols )
    select_sql = f'''\n, final_sql as ( select \n\t{ joincols } \nfrom rename_granularity  order by dbt_valid_from, dbt_valid_to )
    \n'''

    sql = f'''\n---------PHASE 2 create table\n--CREATE OR REPLACE table {args.db}.{args.schema}.{tgt} as \nwith \n\n'''

    final_sql = sql + '\n' + generic_tables + common_hist_sql + join_sql + rename_sql + final_collapse_sql + select_sql

    testing_sql = f'''
--------PHASE 1: DEBUG
-- set Debug to True to test these

select *  from final_sql;  -- verify basically works

-- select UNIQUE_ID_KEY, count(*) as count from final_sql group by UNIQUE_ID_KEY order by count desc;  --find example keys

-- select *  from final_sql where DEBUG_ID like '%DA_KEY%' order by dbt_valid_from, dbt_valid_to ;
-- select *  from tbl0_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial, dbt_valid_to ;
-- select *  from tbl1_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial, dbt_valid_to ;
-- select *  from tbl2_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial, dbt_valid_to ;
-- select *  from tbl3_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial, dbt_valid_to ;
-- select *  from tbl4_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial, dbt_valid_to ;


---------PHASE 2: select for whole table 
-- select * from final_sql;
        '''
    clone_sql = f'''\n----------PHASE 3: CLONE to FINAL LOCATION \n
    -------- check data types
    -- Example of the model:
    --create or replace TABLE DIM_ICD_SNAPSHOT_STEP1 (
    --           CMS_ICD_STATUS_CODE VARCHAR(16777216),
    --           CMS_ICD_STATUS_DESC VARCHAR(16777216),
    --           UNIQUE_ID_KEY VARCHAR(32),
    --           DBT_SCD_ID VARCHAR(32),
    --           DBT_UPDATED_AT TIMESTAMP_NTZ(9),
    --           DBT_VALID_FROM TIMESTAMP_NTZ(9),
    --           DBT_VALID_TO TIMESTAMP_NTZ(9)
    --);

    --create or replace table TGT_SCHEMA.{tgt} clone {args.schema}.{tgt}";'''

    return final_sql + testing_sql + clone_sql


def convert_id( args, idcols ):
    #---switch to using this approach for unique id
    #ETL1 as ( select md5(cast(coalesce(cast(PLACE_OF_SERVICE_CODE as varchar), '') || '-' || coalesce(cast(PLACE_OF_SERVICE_EFFECTIVE_DATE as varchar), '')
    #replace id with UNIQUE_ID_KEY  

    new_cols = []
    for idcol in idcols:
        new_cols.append(f"coalesce(cast( {idcol} as varchar ), '' )")  
    debug_idstr = f"|| '{args.unique_id_key_delim}' || ".join( new_cols )
    idstr = f'md5( {debug_idstr} )'

    return idstr, debug_idstr


def tables_setup( args, tables ):
    for table in tables:
        table[ 'id' ], table[ 'debugid' ] = convert_id( args, table[ 'id' ])
#        table['start']=f"to_timestamp_ntz({table['start']})"
#        table['end']=f" IFNULL(to_timestamp_ntz({table['end']}),TO_DATE('12/31/2999'))"


def main():
    #global variables
    #NOTE: valid future dates will break!, may need to add support for that

    args = types.SimpleNamespace()
    args.db = 'dev_edw'
    args.schema = 'history'
    args.unique_id_key = 'UNIQUE_ID_KEY'
    args.unique_id_key_delim = '-'
    args.dbt_delim = '|'
    args.collapse_delim = '~'
    args.nullval = 'THISISANULLVALplaceholdertoberemoved'
    # etldir = 'I:/IT/ETL/'
    etldir = os.getcwd()

    # basedir = Path( etldir + os.environ[ 'USERNAME' ]+'/Snow_History/history/')
    basedir = Path( etldir + '/Snow_History/history/')
    basedir.mkdir( parents = True, exist_ok = True )
    src_file = basedir/'history.xlsx'

    if not src_file.exists():
        print( 'The Mapping History file (history.xlsx) needs to be placed in:\n', basedir )
        return 0

    for debug in ( True, False ):
        args.debug = debug
        print( 'processing...', src_file)
        for target, tables in load_excel( src_file, 'mapping' ).items():
            fname = target+'.sql'
            if args.debug:
                fname = target+'_debug.txt'
            sql_file = basedir/fname
            tables_setup( args, tables )

            final_sql = merge_hist( args, target, tables )
            print( f'Writing to {basedir} > {fname}')
            # print( 'writing to', sql_file )
            sql_file.write_text (final_sql )

main()

# , CASE WHEN (row_number() over (partition by UNIQUE_ID_KEY order by DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 THEN NULL ELSE DBT_VALID_TO END AS DBT_VALID_TO