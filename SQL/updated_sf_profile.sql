/* These store procedures will require access to the information_schema to generate a list of tables/columns for generating the query loop that is used
to generate the counts for each category.

TOTAL_ROWS                    -- Total number of rows in the table
TOTAL_VALS                    -- Total number of values in the column
TOTAL_BLANKS                  -- Total number of blank/null values
PCT_POP                       -- Percent (rounded) of rows that are populated
PCT_DIST                      -- Percent (rounded) of rows that are distinct
MIN_VAL                       -- Minimum value (includes null/blank values)
MAX_VAL                       -- Maximum value
TOP_RANK                      -- The most frequently observed value (will show one if there is a tie, and may include blanks/null)
TOP_RANK_COUNT                -- The number of times the top ranked value occurs in the column
NONBLANK_TOP_RANK             -- The most frequently observed non-blank value in a column
NONBLANK_TOP_RANK_COUNT       -- The number of times the nonblank top ranked value occurs in the column
NONBLANK_TOP_RANK_LENGTH      -- The length of the non-blank top-ranked column value
BOTTOM_RANK                   -- The least frequently observed value (includes blank/null)
BOTTOM_RANK_COUNT             -- The number of times the bottom ranked value occurs in the column
BOTTOM_RANK_LENGTH            -- The length of the bottom ranked column value


*/

create or replace procedure TABLE_PROFILE (DBNAME TEXT, TSCHEMA TEXT, TNAME TEXT)
    RETURNS TABLE ( TABLE_DB text, TABLE_SCHEMA text, TABLE_NAME text, FIELD_NAME text, TOTAL_ROWS number, TOTAL_VALS number, TOTAL_BLANKS number, PCT_POP number, PCT_DIST number, MIN_VAL text, MAX_VAL text, TOP_RANK text, TOP_RANK_COUNT number, NONBLANK_TOP_RANK text,  NONBLANK_TOP_RANK_COUNT number, NONBLANK_TOP_RANK_LENGTH number, BOTTOM_RANK text, BOTTOM_RANK_COUNT number, BOTTOM_RANK_LENGTH number )
    LANGUAGE SQL
    EXECUTE AS CALLER
    as
declare
    rs resultset;
    rs2 resultset;
    rs_output resultset;
begin
    -- get relevant tables
    let db := :DBNAME;
    let db_catalog := db||'.INFORMATION_SCHEMA.columns';
    let table_schema := :TSCHEMA;
    let table_name := :TNAME;
    
    rs := (select table_catalog, table_schema, table_name, column_name, data_type 
           from IDENTIFIER(:db_catalog)
             where TABLE_SCHEMA != 'INFORMATION_SCHEMA'
               and TABLE_SCHEMA = :table_schema and table_name = :table_name);

    let tmp_array := ARRAY_CONSTRUCT();
    let table_count :=0;let i:=0;
    for arow_rs in rs do
        i:=i+1;

        let adb     := '"'|| arow_rs.table_catalog::TEXT || '"';
        let aschema := '"'|| arow_rs.table_schema::TEXT || '"';
        let atable  := '"'|| arow_rs.table_name::TEXT ||'"';
        let acol    := '"'|| arow_rs.column_name::TEXT ||'"';
        let adt     := arow_rs.data_type::TEXT;
        let qualified_table := adb||'.'||aschema||'.'||atable;     

        let query := 'with DIST as ( select '''||adb||''' as table_db, '''||aschema||''' as table_schema, '''||atable||''' as table_name, '''||acol||''' as fld_nm, '||acol||'  as fld_val, count(*) as numrows, sum( case when trim(nvl(to_char(fld_val),'''')) ='''' then 1 else 0 end ) as numblanks from ' ||qualified_table|| ' group by fld_val ), RANKING as ( select table_db, table_schema, table_name, fld_nm, fld_val, numrows, numblanks, dense_rank() over(order by numrows desc) as rank_asc, dense_rank() over(order by (case when fld_val is null then -1 else numrows end)  desc) as nonblank_rank_asc, dense_rank() over( order by numrows) as rank_desc from DIST ) select current_date, table_db, table_schema, table_name, fld_nm , sum(numrows) as total_rows,count(distinct fld_val) as total_vals, sum(numblanks) as total_blanks, min( fld_val ) as min_val, max( fld_val ) as max_val, max( case when rank_asc = 1 then fld_val else null end ) as top_rank, max( case when rank_asc = 1 then numrows else 0 end ) as top_rank_count, max( case when nonblank_rank_asc = 1 then fld_val else null end ) as nonblank_top_rank, max( case when nonblank_rank_asc = 1 then numrows else 0 end ) as nonblank_top_rank_count, length( nonblank_top_rank ) as nonblank_top_rank_length, max( case when rank_desc = 1 then fld_val else null end ) as bottom_rank, max( case when rank_desc = 1 then numrows else 0 end ) as bottom_rank_count, length( bottom_rank ) as bottom_rank_length from RANKING  group by table_db, table_schema, table_name, fld_nm';
        rs2 :=  ( execute immediate :query    );

        for arow_rs2 in rs2 do
          let table_db           := arow_rs2.table_db::text;
          let table_schema       := arow_rs2.table_schema::text;
          let field_name         := arow_rs2.fld_nm::text;
          let table_name         := arow_rs2.table_name::text;
          let total_rows         := arow_rs2.total_rows::number;
          let total_vals         := arow_rs2.total_vals::number;
          let total_blanks       := arow_rs2.total_blanks::number;
          let min_val            := arow_rs2.min_val::text;
          let max_val            := arow_rs2.max_val::text;
          let top_rank           := arow_rs2.top_rank::text;
          let top_rank_count     := arow_rs2.top_rank_count::number;
          let nonblank_top_rank  := arow_rs2.nonblank_top_rank::text;
          let nonblank_top_rank_count      := arow_rs2.nonblank_top_rank_count::number;
          let nonblank_top_rank_length     := arow_rs2.nonblank_top_rank_length::number; 
          let bottom_rank           := arow_rs2.bottom_rank::text;
          let bottom_rank_count     := arow_rs2.bottom_rank_count::number;
          let bottom_rank_length    := arow_rs2.bottom_rank_length::number;
          let obj       :=  OBJECT_CONSTRUCT('table_db',table_db, 'table_schema',table_schema, 'table_name',table_name, 'field_name',field_name, 'total_rows',total_rows, 'total_vals',total_vals, 'total_blanks',total_blanks, 'min_val',min_val, 'max_val',max_val, 'top_rank',top_rank, 'top_rank_count',top_rank_count, 'nonblank_top_rank',nonblank_top_rank, 'nonblank_top_rank_count',nonblank_top_rank_count, 'nonblank_top_rank_length',nonblank_top_rank_length, 'bottom_rank',bottom_rank, 'bottom_rank_count',bottom_rank_count,  'bottom_rank_length',bottom_rank_length );
          tmp_array := array_append(:tmp_array, obj );
        end for;

    end for;

    rs_output := (select 
                    value:table_db::text                    as table_db,
                    value:table_schema::text                as table_schema,
                    value:table_name::text                  as table_name,
                    value:field_name::text                  ||
                    IFF(value:total_vals::number=1,'  *','')  as field_name,
                    value:total_rows::number                as total_rows,
                    value:total_vals::number                as total_vals,
                    value:total_blanks::number              as total_blanks,
                    trunc( ((total_rows-total_blanks) / total_rows )*100, 2)    as pct_pop,
                    trunc( (total_vals / total_rows)*100, 2)    as pct_dist,
                    value:min_val::text                     as min_val,
                    value:max_val::text                     as max_val,
                    value:top_rank::text                    as top_rank,                  
                    value:top_rank_count::number            as top_rank_count,                  
                    value:nonblank_top_rank::text           as nonblank_top_rank,                  
                    value:nonblank_top_rank_count::number   as nonblank_top_rank_count,
                    value:nonblank_top_rank_length::number  as nonblank_top_rank_length,
                    value:bottom_rank::text                 as bottom_rank,                  
                    value:bottom_rank_count::number         as bottom_rank_count,                  
                    value:bottom_rank_length::number        as bottom_rank_length
                  from table(flatten(:tmp_array))
                 order by field_name);
    return table(rs_output);
end;

grant usage on PROCEDURE TABLE_PROFILE(varchar, varchar, varchar) TO PUBLIC;

use secondary roles all;

call TABLE_PROFILE ('EDP_BRONZE_DEV','DEV_TEST_DV', 'DIRECT_SPEND_CAT_SRO');








create or replace procedure SCHEMA_PROFILE (DBNAME TEXT, TSCHEMA TEXT)
    RETURNS TABLE ( TABLE_DB text, TABLE_SCHEMA text, TABLE_NAME text, FIELD_NAME text, TOTAL_ROWS number, TOTAL_VALS number, TOTAL_BLANKS number, PCT_POP number, PCT_DIST number, MIN_VAL text, MAX_VAL text, TOP_RANK text, TOP_RANK_COUNT number, NONBLANK_TOP_RANK text,  NONBLANK_TOP_RANK_COUNT number, NONBLANK_TOP_RANK_LENGTH number, BOTTOM_RANK text, BOTTOM_RANK_COUNT number, BOTTOM_RANK_LENGTH number )
    LANGUAGE SQL
    EXECUTE AS CALLER
    as
declare
    rs resultset;
    rs2 resultset;
    rs_output resultset;
begin
    -- get relevant tables
    let db := :DBNAME;
    let db_catalog := db||'.INFORMATION_SCHEMA.columns';
    let table_schema := :TSCHEMA;
    
    rs := (select table_catalog, table_schema, table_name, column_name, data_type 
           from IDENTIFIER(:db_catalog)
             where TABLE_SCHEMA != 'INFORMATION_SCHEMA'
               and TABLE_SCHEMA = :table_schema);
               
    let tmp_array := ARRAY_CONSTRUCT();
    let table_count :=0;let i:=0;
    for arow_rs in rs do
        i:=i+1;

        let adb     := '"'|| arow_rs.table_catalog::TEXT || '"';
        let aschema := '"'|| arow_rs.table_schema::TEXT || '"';
        let atable  := '"'|| arow_rs.table_name::TEXT ||'"';
        let acol    := '"'|| arow_rs.column_name::TEXT ||'"';
        let adt     := arow_rs.data_type::TEXT;
        let qualified_table := adb||'.'||aschema||'.'||atable;     

        let query := 'with DIST as ( select '''||adb||''' as table_db, '''||aschema||''' as table_schema, '''||atable||''' as table_name, '''||acol||''' as fld_nm, '||acol||'  as fld_val, count(*) as numrows, sum( case when trim(nvl(to_char(fld_val),'''')) ='''' then 1 else 0 end ) as numblanks from ' ||qualified_table|| ' group by fld_val ), RANKING as ( select table_db, table_schema, table_name, fld_nm, fld_val, numrows, numblanks, dense_rank() over(order by numrows desc) as rank_asc, dense_rank() over(order by (case when fld_val is null then -1 else numrows end)  desc) as nonblank_rank_asc, dense_rank() over( order by numrows) as rank_desc from DIST ) select current_date, table_db, table_schema, table_name, fld_nm , sum(numrows) as total_rows,count(distinct fld_val) as total_vals, sum(numblanks) as total_blanks, min( fld_val ) as min_val, max( fld_val ) as max_val, max( case when rank_asc = 1 then fld_val else null end ) as top_rank, max( case when rank_asc = 1 then numrows else 0 end ) as top_rank_count, max( case when nonblank_rank_asc = 1 then fld_val else null end ) as nonblank_top_rank, max( case when nonblank_rank_asc = 1 then numrows else 0 end ) as nonblank_top_rank_count, length( nonblank_top_rank ) as nonblank_top_rank_length, max( case when rank_desc = 1 then fld_val else null end ) as bottom_rank, max( case when rank_desc = 1 then numrows else 0 end ) as bottom_rank_count, length( bottom_rank ) as bottom_rank_length from RANKING  group by table_db, table_schema, table_name, fld_nm';
        rs2 :=  ( execute immediate :query    );

        for arow_rs2 in rs2 do
          let table_db           := arow_rs2.table_db::text;
          let table_schema       := arow_rs2.table_schema::text;
          let field_name         := arow_rs2.fld_nm::text;
          let table_name         := arow_rs2.table_name::text;
          let total_rows         := arow_rs2.total_rows::number;
          let total_vals         := arow_rs2.total_vals::number;
          let total_blanks       := arow_rs2.total_blanks::number;
          let min_val            := arow_rs2.min_val::text;
          let max_val            := arow_rs2.max_val::text;
          let top_rank           := arow_rs2.top_rank::text;
          let top_rank_count     := arow_rs2.top_rank_count::number;
          let nonblank_top_rank  := arow_rs2.nonblank_top_rank::text;
          let nonblank_top_rank_count      := arow_rs2.nonblank_top_rank_count::number;
          let nonblank_top_rank_length     := arow_rs2.nonblank_top_rank_length::number; 
          let bottom_rank           := arow_rs2.bottom_rank::text;
          let bottom_rank_count     := arow_rs2.bottom_rank_count::number;
          let bottom_rank_length    := arow_rs2.bottom_rank_length::number;
          let obj       :=  OBJECT_CONSTRUCT('table_db',table_db, 'table_schema',table_schema, 'table_name',table_name, 'field_name',field_name, 'total_rows',total_rows, 'total_vals',total_vals, 'total_blanks',total_blanks, 'min_val',min_val, 'max_val',max_val, 'top_rank',top_rank, 'top_rank_count',top_rank_count, 'nonblank_top_rank',nonblank_top_rank, 'nonblank_top_rank_count',nonblank_top_rank_count, 'nonblank_top_rank_length',nonblank_top_rank_length, 'bottom_rank',bottom_rank, 'bottom_rank_count',bottom_rank_count,  'bottom_rank_length',bottom_rank_length );
          tmp_array := array_append(:tmp_array, obj );
        end for;

    end for;

    rs_output := (select 
                    value:table_db::text                    as table_db,
                    value:table_schema::text                as table_schema,
                    value:table_name::text                  as table_name,
                    value:field_name::text                  ||
                    IFF(value:total_vals::number=1,'  *','')  as field_name,
                    value:total_rows::number                as total_rows,
                    value:total_vals::number                as total_vals,
                    value:total_blanks::number              as total_blanks,
                    trunc( ((total_rows-total_blanks) / total_rows )*100, 2)    as pct_pop,
                    trunc( (total_vals / total_rows)*100, 2)    as pct_dist,
                    value:min_val::text                     as min_val,
                    value:max_val::text                     as max_val,
                    value:top_rank::text                    as top_rank,                  
                    value:top_rank_count::number            as top_rank_count,                  
                    value:nonblank_top_rank::text           as nonblank_top_rank,                  
                    value:nonblank_top_rank_count::number   as nonblank_top_rank_count,
                    value:nonblank_top_rank_length::number  as nonblank_top_rank_length,
                    value:bottom_rank::text                 as bottom_rank,                  
                    value:bottom_rank_count::number         as bottom_rank_count,                  
                    value:bottom_rank_length::number        as bottom_rank_length
                  from table(flatten(:tmp_array))
                 order by field_name);
    return table(rs_output);
end;

--Updated SCHEMA_PROFILE to not include views
CREATE OR REPLACE PROCEDURE PLAYGROUND.ROBERT_METZKER.SCHEMA_PROFILE("DBNAME" VARCHAR(16777216), "TSCHEMA" VARCHAR(16777216))
RETURNS TABLE ("TABLE_DB" VARCHAR(16777216), "TABLE_SCHEMA" VARCHAR(16777216), "TABLE_NAME" VARCHAR(16777216), "FIELD_NAME" VARCHAR(16777216), "TOTAL_ROWS" NUMBER(38,0), "TOTAL_VALS" NUMBER(38,0), "TOTAL_BLANKS" NUMBER(38,0), "PCT_POP" NUMBER(38,0), "PCT_DIST" NUMBER(38,0), "MIN_VAL" VARCHAR(16777216), "MAX_VAL" VARCHAR(16777216), "TOP_RANK" VARCHAR(16777216), "TOP_RANK_COUNT" NUMBER(38,0), "NONBLANK_TOP_RANK" VARCHAR(16777216), "NONBLANK_TOP_RANK_COUNT" NUMBER(38,0), "NONBLANK_TOP_RANK_LENGTH" NUMBER(38,0), "BOTTOM_RANK" VARCHAR(16777216), "BOTTOM_RANK_COUNT" NUMBER(38,0), "BOTTOM_RANK_LENGTH" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS CALLER
AS 'declare
    rs resultset;
    rs2 resultset;
    rs_output resultset;
begin
    -- get relevant tables
    let db := :DBNAME;
    let tbl_catalog := db||''.INFORMATION_SCHEMA.tables'';
    let db_catalog := db||''.INFORMATION_SCHEMA.columns'';
    let table_schema := :TSCHEMA;
    
    rs := (with tbls as (
    select table_catalog, table_schema, table_name from IDENTIFIER(:tbl_catalog)
    where table_type <> ''VIEW'' and TABLE_SCHEMA = :table_schema )
        select col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type
        from IDENTIFIER(:db_catalog) col 
        JOIN tbls on ( tbls.table_catalog = col.table_catalog and tbls.table_schema = col.table_schema and tbls.table_name = col.table_name));
               
    let tmp_array := ARRAY_CONSTRUCT();
    let table_count :=0;let i:=0;
    for arow_rs in rs do
        i:=i+1;

        let adb     := ''"''|| arow_rs.table_catalog::TEXT || ''"'';
        let aschema := ''"''|| arow_rs.table_schema::TEXT || ''"'';
        let atable  := ''"''|| arow_rs.table_name::TEXT ||''"'';
        let acol    := ''"''|| arow_rs.column_name::TEXT ||''"'';
        let adt     := arow_rs.data_type::TEXT;
        let qualified_table := adb||''.''||aschema||''.''||atable;     

        let query := ''with DIST as ( select ''''''||adb||'''''' as table_db, ''''''||aschema||'''''' as table_schema, ''''''||atable||'''''' as table_name, ''''''||acol||'''''' as fld_nm, ''||acol||''  as fld_val, count(*) as numrows, sum( case when trim(nvl(to_char(fld_val),'''''''')) ='''''''' then 1 else 0 end ) as numblanks from '' ||qualified_table|| '' group by fld_val ), RANKING as ( select table_db, table_schema, table_name, fld_nm, fld_val, numrows, numblanks, dense_rank() over(order by numrows desc) as rank_asc, dense_rank() over(order by (case when fld_val is null then -1 else numrows end)  desc) as nonblank_rank_asc, dense_rank() over( order by numrows) as rank_desc from DIST ) select current_date, table_db, table_schema, table_name, fld_nm , sum(numrows) as total_rows,count(distinct fld_val) as total_vals, sum(numblanks) as total_blanks, min( fld_val ) as min_val, max( fld_val ) as max_val, max( case when rank_asc = 1 then fld_val else null end ) as top_rank, max( case when rank_asc = 1 then numrows else 0 end ) as top_rank_count, max( case when nonblank_rank_asc = 1 then fld_val else null end ) as nonblank_top_rank, max( case when nonblank_rank_asc = 1 then numrows else 0 end ) as nonblank_top_rank_count, length( nonblank_top_rank ) as nonblank_top_rank_length, max( case when rank_desc = 1 then fld_val else null end ) as bottom_rank, max( case when rank_desc = 1 then numrows else 0 end ) as bottom_rank_count, length( bottom_rank ) as bottom_rank_length from RANKING  group by table_db, table_schema, table_name, fld_nm'';
        rs2 :=  ( execute immediate :query    );

        for arow_rs2 in rs2 do
          let table_db           := arow_rs2.table_db::text;
          let table_schema       := arow_rs2.table_schema::text;
          let field_name         := arow_rs2.fld_nm::text;
          let table_name         := arow_rs2.table_name::text;
          let total_rows         := arow_rs2.total_rows::number;
          let total_vals         := arow_rs2.total_vals::number;
          let total_blanks       := arow_rs2.total_blanks::number;
          let min_val            := arow_rs2.min_val::text;
          let max_val            := arow_rs2.max_val::text;
          let top_rank           := arow_rs2.top_rank::text;
          let top_rank_count     := arow_rs2.top_rank_count::number;
          let nonblank_top_rank  := arow_rs2.nonblank_top_rank::text;
          let nonblank_top_rank_count      := arow_rs2.nonblank_top_rank_count::number;
          let nonblank_top_rank_length     := arow_rs2.nonblank_top_rank_length::number; 
          let bottom_rank           := arow_rs2.bottom_rank::text;
          let bottom_rank_count     := arow_rs2.bottom_rank_count::number;
          let bottom_rank_length    := arow_rs2.bottom_rank_length::number;
          let obj       :=  OBJECT_CONSTRUCT(''table_db'',table_db, ''table_schema'',table_schema, ''table_name'',table_name, ''field_name'',field_name, ''total_rows'',total_rows, ''total_vals'',total_vals, ''total_blanks'',total_blanks, ''min_val'',min_val, ''max_val'',max_val, ''top_rank'',top_rank, ''top_rank_count'',top_rank_count, ''nonblank_top_rank'',nonblank_top_rank, ''nonblank_top_rank_count'',nonblank_top_rank_count, ''nonblank_top_rank_length'',nonblank_top_rank_length, ''bottom_rank'',bottom_rank, ''bottom_rank_count'',bottom_rank_count,  ''bottom_rank_length'',bottom_rank_length );
          tmp_array := array_append(:tmp_array, obj );
        end for;

    end for;

    rs_output := (select 
                    value:table_db::text                    as table_db,
                    value:table_schema::text                as table_schema,
                    value:table_name::text                  as table_name,
                    value:field_name::text                  ||
                    IFF(value:total_vals::number=1,''  *'','''')  as field_name,
                    value:total_rows::number                as total_rows,
                    value:total_vals::number                as total_vals,
                    value:total_blanks::number              as total_blanks,
                    trunc( ((total_rows-total_blanks) / total_rows )*100, 2)    as pct_pop,
                    trunc( (total_vals / total_rows)*100, 2)    as pct_dist,
                    value:min_val::text                     as min_val,
                    value:max_val::text                     as max_val,
                    value:top_rank::text                    as top_rank,                  
                    value:top_rank_count::number            as top_rank_count,                  
                    value:nonblank_top_rank::text           as nonblank_top_rank,                  
                    value:nonblank_top_rank_count::number   as nonblank_top_rank_count,
                    value:nonblank_top_rank_length::number  as nonblank_top_rank_length,
                    value:bottom_rank::text                 as bottom_rank,                  
                    value:bottom_rank_count::number         as bottom_rank_count,                  
                    value:bottom_rank_length::number        as bottom_rank_length
                  from table(flatten(:tmp_array))
                 order by field_name);
    return table(rs_output);
end';

use secondary roles all;

call SCHEMA_PROFILE ('EDP_BRONZE_DEV','DEV_TEST_DV' );


grant usage on PROCEDURE SCHEMA_PROFILE(varchar, varchar) TO PUBLIC;

select distinct role_name from information_schema.applicable_roles order by role_name;
