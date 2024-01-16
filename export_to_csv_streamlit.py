# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import time

def generate_col_sql( session ):
    database = st.session_state.db
    schema = st.session_state.schema
    table = st.session_state.table
    cols=""
    run_sql = f"""
            with cols_agg as 
            (select 
                table_catalog, table_schema, table_name, 
                LISTAGG(
                case 
                    when data_type in ('TEXT','STRING','VARCHAR') THEN
                    'replace( replace( ' || column_name || ', \\'"\\', \\'\\'), char(10), \\'\\') as ' || column_name
                else
                    column_name
                end, ', ') as col_list
            FROM {database}.information_schema.columns
            where table_catalog = upper( '{database}' )
            and table_schema = UPPER( '{schema}' )
            and table_name   = UPPER(  '{table}' )
            group by table_catalog, table_schema, table_name
            )
        select col_list from cols_agg"""

    col_list = session.sql(run_sql)
    col_df = col_list.to_pandas()
    col_list = col_df['COL_LIST'][0] 
    
    # Uncomment for troubleshooting
    if st.session_state.debug:
        st.code( run_sql, language='sql')
        st.text( f"Generated: {col_list}"  )
    
    return col_list
    
    
def export_table_to_csv( session, cols ):
    stage = st.session_state.stage
    database = st.session_state.db
    schema = st.session_state.schema
    table = st.session_state.table

    run_sql = f"""
    COPY INTO @{stage}/{table}.csv 
    FROM (select {cols} 
    from {database}.{schema}.{table} ) 
    FILE_FORMAT= (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION=NONE) SINGLE=TRUE header=true
    """

    st.code( run_sql, language='sql')

    # Export the selected table to CSV when clicked    
    if len(cols) >1:
        st.subheader(f"Confirm Export:")
        st.caption( f"{schema}.{table} to {stage}")
        # st.code( cols, language='sql')
        
        if st.button("EXPORT", type="primary" ):   
            with st.spinner(f'Fetching Schemas for {chosen_db}...'):
                time.sleep(2)
        
            session.sql(run_sql)        
            st.success("Export Complete")

def main():
    # Write directly to the app
    st.title("Export CSV to STAGE")
    st.session_state.debug = True
    st.session_state
    cols = ""
    # key_states
    
    st.write(
        """Exports a table to CSV, replacing text columns with a cleansed version,
        removing all double quotes and line feeds and double quoting the entire text value.
        """
    )
    
    # Get the current credentials
    session = get_active_session()
    
    # Choose a stage
    stage = st.text_input( "Please Enter a fully qualified stage", 
                  "data_governance.public.unload_to_azure",
                 help="fully qualify the stage as database.schema.stagename",
                 key="stage")
    
    # Display a list of Databases to allow Exports from
    chosen_db = st.selectbox(
        'Which Database would you like to extract from?',
        ('DATA_GOVERNANCE', 'PLAYGROUND', 'EDP_BRONZE_DEV'),
        index=0,
        key="db"
    )

    # Display a list of Database Schemas within the Database
    if len(chosen_db) >=1:
        run_sql = f"""select schema_name from {chosen_db}.information_schema.schemata """
        # Uncomment for troubleshooting
        if st.session_state.debug:
            st.code( run_sql, language='sql')
        
        schema_results = session.sql(run_sql)
        schema_df = schema_results.to_pandas()
        with st.spinner(f'Fetching Schemas for {chosen_db}...'):
            time.sleep(2)
    
        chosen_schema = st.selectbox(
            'Which Schema would you like to extract from?',
            schema_results,
            key="schema"
        )

        
        # Display a list of Database Tables with the Schema
        if st.session_state.table == "":
            chosen_db = st.session_state.db
            chosen_schema = st.session_state.schema
            run_sql = f"""select table_name from {chosen_db}.information_schema.tables """
            run_sql+= f"""where table_schema = upper('{chosen_schema}') """
            # Uncomment for troubleshooting
            if st.session_state.debug:
                st.code( run_sql, language='sql')
            
            table_results = session.sql(run_sql)
            table_df = table_results.to_pandas()
            
            with st.spinner(f'Fetching Tables for {chosen_schema}...'):
                time.sleep(2)
        
                chosen_table = st.selectbox(
                    'Which Table would you like to extract from?',
                    table_results,
                    key="table"
                )
    
            qualified_table = f"""{chosen_db}.{chosen_schema}.{chosen_table}"""
        else:
            if st.session_state.table:
                chosen_db = st.session_state.db
                chosen_schema = st.session_state.schema
                chosen_table = st.session_state.table
                
                cols = generate_col_sql( session  )
                export_table_to_csv( session, cols )
        
    st.session_state        

if __name__ == '__main__':
    key_states = ['debug','enabled', 'has_table' ,'table']
    
    for key in key_states:
        if key not in st.session_state:
            st.session_state[key] = ""
    
    main()

