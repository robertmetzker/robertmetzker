CREATE OR REPLACE PROCEDURE DATA_GOVERNANCE.UTILITIES.TABLE_PROFILE2("DBNAME" VARCHAR(16777216), "TSCHEMA" VARCHAR(16777216), "TNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
    rs resultset;
    query string;
begin
    -- Get relevant columns
    let db := :DBNAME;
    let db_catalog := db || ''.INFORMATION_SCHEMA.columns'';
    let table_schema := :TSCHEMA;
    let table_name := :TNAME;

    rs := (select table_catalog, table_schema, table_name, column_name, data_type,
                  case when lower(column_name) not like ''%_id'' and data_type in (''NUMBER'', ''INTEGER'', ''FLOAT'', ''DOUBLE'', ''REAL'', ''DECIMAL'') then ''NUMERIC'' else ''NON_NUMERIC'' end as column_type
           from IDENTIFIER(:db_catalog)
           where TABLE_SCHEMA != ''INFORMATION_SCHEMA''
             and TABLE_SCHEMA = :table_schema 
             and table_name = :table_name);

    for arow_rs in rs do
        let adb := arow_rs.table_catalog::TEXT;
        let aschema := arow_rs.table_schema::TEXT;
        let atable := arow_rs.table_name::TEXT;
        let acol := '''' || arow_rs.column_name::TEXT || '''';
        let qualified_table := adb || ''.'' || aschema || ''.'' || atable;
        let data_type := arow_rs.data_type::TEXT;

        let std_dev_sql := case when arow_rs.column_type = ''NUMERIC'' then ''stddev(TRY_TO_NUMBER('' || acol || '')) as std_dev'' else ''NULL as std_dev'' end;
        let numeric_sum_sql := case when arow_rs.column_type = ''NUMERIC'' then ''sum(TRY_TO_NUMBER('' || acol || '')) as numeric_sum'' else ''NULL as numeric_sum'' end;
        let numeric_mean_sql := case when arow_rs.column_type = ''NUMERIC'' then ''avg(TRY_TO_NUMBER('' || acol || '')) as numeric_mean'' else ''NULL as numeric_mean'' end;

        query := ''
        INSERT INTO DATA_GOVERNANCE.METADATA.TABLE_PROFILE
        WITH DIST AS (
            SELECT '''''' || adb || '''''' AS table_db, 
                   '''''' || aschema || '''''' AS table_schema, 
                   '''''' || atable || '''''' AS table_name, 
                   ''''"'' || acol || ''"'''' AS fld_nm, 
                   '''''' || data_type || '''''' AS data_type, 
                   '' || acol || '' AS fld_val, 
                   COUNT(*) AS numrows,
                   SUM(CASE WHEN TRIM(NVL(TO_CHAR('' || acol || ''), '''''''')) = '''''''' THEN 1 ELSE 0 END) AS numblanks
            FROM '' || qualified_table || '' 
            GROUP BY '' || acol || ''
        ),
        RANKING AS (
            SELECT table_db, table_schema, table_name, fld_nm, data_type, fld_val, numrows, numblanks,
                   DENSE_RANK() OVER(ORDER BY numrows DESC) AS rank_asc,
                   DENSE_RANK() OVER(ORDER BY (CASE WHEN fld_val IS NULL THEN -1 ELSE numrows END) DESC) AS nonblank_rank_asc,
                   DENSE_RANK() OVER(ORDER BY numrows) AS rank_desc
            FROM DIST
        ),
        FINAL AS (
            SELECT 
                CURRENT_DATE AS run_date, 
                table_db, 
                table_schema, 
                table_name, 
                fld_nm, 
                data_type,
                SUM(numrows) AS total_rows,
                COUNT(DISTINCT fld_val) AS total_vals, 
                SUM(numblanks) AS total_blanks,
                TRUNC(((SUM(numrows)-SUM(numblanks)) / SUM(numrows))*100, 2) AS pct_pop,
                TRUNC((COUNT(DISTINCT fld_val) / SUM(numrows))*100, 2) AS pct_dist,
                MIN(fld_val) AS min_val, 
                MAX(fld_val) AS max_val, 
                MAX(CASE WHEN rank_asc = 1 THEN fld_val ELSE NULL END) AS top_rank,
                MAX(CASE WHEN rank_asc = 1 THEN numrows ELSE 0 END) AS top_rank_count,
                MAX(CASE WHEN nonblank_rank_asc = 1 THEN fld_val ELSE NULL END) AS nonblank_top_rank,
                MAX(CASE WHEN nonblank_rank_asc = 1 THEN numrows ELSE 0 END) AS nonblank_top_rank_count,
                LENGTH(MAX(CASE WHEN nonblank_rank_asc = 1 THEN fld_val ELSE NULL END)) AS nonblank_top_rank_length,
                MAX(CASE WHEN rank_desc = 1 THEN fld_val ELSE NULL END) AS bottom_rank,
                MAX(CASE WHEN rank_desc = 1 THEN numrows ELSE 0 END) AS bottom_rank_count,
                LENGTH(MAX(CASE WHEN rank_desc = 1 THEN fld_val ELSE NULL END)) AS bottom_rank_length,
                AVG(LENGTH(fld_val)) AS avg_length, 
                '' || std_dev_sql || '', 
                COUNT(DISTINCT fld_val) AS unique_count, 
                TRUNC((MAX(CASE WHEN rank_asc = 1 THEN numrows ELSE 0 END) / SUM(numrows))*100, 2) AS top_rank_percent,
                TRUNC((MAX(CASE WHEN rank_desc = 1 THEN numrows ELSE 0 END) / SUM(numrows))*100, 2) AS bottom_rank_percent,
                '' || numeric_sum_sql || '', 
                '' || numeric_mean_sql || ''
            FROM RANKING
            GROUP BY table_db, table_schema, table_name, fld_nm, data_type
        )
        SELECT * FROM FINAL'';

        execute immediate :query;
    end for;

    return ''Table profiling data has been successfully inserted into DATA_GOVERNANCE.METADATA.TABLE_PROFILE'';
end;
';
