CREATE OR REPLACE PROCEDURE COMPARE_SCHEMAS(SRC_DBSCHEMA STRING, TGT_DBSCHEMA STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  -- Variables to hold the database and schema names.
  src_db STRING;
  src_schema STRING;
  tgt_db STRING;
  tgt_schema STRING;

  -- Variables to hold the source and target names for the comparison.
  src_name STRING;
  tgt_name STRING;

  -- Variable to construct dynamic SQL statements.
  exec_sql STRING;
  
BEGIN
  -- Split the database and schema names.
  src_db := SPLIT_PART(SRC_DBSCHEMA, '.', 1);
  src_schema := SPLIT_PART(SRC_DBSCHEMA, '.', 2);
  tgt_db := SPLIT_PART(TGT_DBSCHEMA, '.', 1);
  tgt_schema := SPLIT_PART(TGT_DBSCHEMA, '.', 2);

  -- Determine the source and target names for the comparison.
  IF (src_schema = tgt_schema) THEN
    src_name := src_db;
    tgt_name := tgt_db;
  ELSE
    src_name := src_schema;
    tgt_name := tgt_schema;
  END IF;

  -- Generate SQL for the comparison query.
  exec_sql := '
  CREATE OR REPLACE TEMPORARY TABLE COMPARE_RESULTS AS 
    WITH SRC AS (
        SELECT 
            ''D'' AS IND
            , TABLE_SCHEMA
            , TABLE_NAME
            , COLUMN_NAME
            , DATA_TYPE
            , IFF( NUMERIC_PRECISION IS NOT NULL, NUMERIC_PRECISION||''.''||NUMERIC_SCALE, '''') AS PS
        FROM  ' || src_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '''|| src_schema ||'''
    ),
    TGT AS (
        SELECT 
            ''P'' AS IND
            , TABLE_SCHEMA
            , TABLE_NAME
            , COLUMN_NAME
            , DATA_TYPE
            , IFF( NUMERIC_PRECISION IS NOT NULL, NUMERIC_PRECISION||''.''||NUMERIC_SCALE, '''') AS PS
        FROM  ' || tgt_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '''|| tgt_schema ||'''
    ),
    COMPARE AS (
        SELECT    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IND
                , DATA_TYPE || IFF( PS <>'''', ''(''||PS||'')'', '''') AS DT
            FROM SRC
        UNION ALL
        SELECT    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IND
                , DATA_TYPE || IFF( PS<>'''', ''(''||PS||'')'', '''') AS DT
            FROM TGT
    ),
    DIFF as (
        select
              TABLE_NAME
            , COLUMN_NAME
            , max( case when IND = ''P'' then ''X'' else ''-'' end ) as ' || tgt_name || '_IND
            , max( case when IND = ''D'' then ''X'' else ''-'' end ) as ' || src_name || '_IND
            , max( case when IND = ''P'' then DT  else ''-'' end ) as ' || tgt_name || '_TYPE
            , max( case when IND = ''D'' then DT  else ''-'' end ) as ' || src_name || '_TYPE
            , max( case when IND = ''P'' then DT || '' -> '' else ''+ '' end ) ||
              max( case when IND = ''D'' then DT else ''-'' end )  as TYPE
    from COMPARE
    group by TABLE_NAME, COLUMN_NAME
    having ' || src_name || '_IND <> ' || tgt_name || '_IND or ' || tgt_name || '_TYPE <> ' || src_name || '_TYPE 
    )
    select TABLE_NAME, COLUMN_NAME, ' || tgt_name || '_IND, ' || src_name || '_IND, TYPE 
      from DIFF
    order by 1,2;
        ';
    
  -- Execute the generated SQL.
  EXECUTE IMMEDIATE exec_sql;
  
  -- Return a success message.
  RETURN 'Comparison query executed successfully. Check the COMPARE_RESULTS temporary table for the results.';
END;
$$;

CALL COMPARE_SCHEMAS('DATAVAULT_QA.RAW_VAULT', 'DATAVAULT_PROD.RAW_VAULT');
CALL COMPARE_SCHEMAS('PLAYGROUND.PLAYGROUND_CICD_QA', 'PLAYGROUND.PLAYGROUND_CICD');

select * from COMPARE_RESULTS;