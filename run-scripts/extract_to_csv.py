# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time
from datetime import datetime

def display_table_info(session, chosen_db, chosen_schema, chosen_table):
    run_sql = f"""
        SELECT table_name, row_count, trunc( (bytes/1024)/1024, 2) as MB
        FROM {chosen_db}.information_schema.tables
        WHERE table_schema = '{chosen_schema}' AND table_name = '{chosen_table}'
    """
    table_info_df = session.sql(run_sql).to_pandas()

    if not table_info_df.empty:
        st.subheader(f"Expected Summary for: {chosen_schema}.{chosen_table}")
        st.dataframe(table_info_df)
        
        # Check if 'MB' is greater than 100 and raise a warning
        if table_info_df['MB'].iloc[0] >= 100:
            st.warning("Warning: Table size is greater than 100 MB.")
    else:
        st.error("No table information available for the selected table.")

def generate_col_sql(session, database, schema, table):
    run_sql = f"""
        WITH cols_agg AS 
        (SELECT 
            table_catalog, table_schema, table_name, 
            LISTAGG(
            CASE 
                WHEN data_type IN ('TEXT','STRING','VARCHAR') THEN
                'replace(replace(' || column_name || ', \\'"\\', \\'\\'), char(10), \\'\\') as ' || column_name
            ELSE
                column_name
            END, ', ') AS col_list
        FROM {database}.information_schema.columns
        WHERE table_catalog = UPPER('{database}')
        AND table_schema = UPPER('{schema}')
        AND table_name = UPPER('{table}')
        GROUP BY table_catalog, table_schema, table_name
        )
    SELECT col_list FROM cols_agg"""

    col_list = session.sql(run_sql).to_pandas()['COL_LIST'][0]
    
    if st.session_state.debug:
        st.code(run_sql, language='sql')
        st.text(f"Generated: {col_list}")
    
    return col_list

def export_table_to_csv(session, cols, database, schema, table, stage):
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Append the current date and time to the CSV file name for uniqueness
    csv_file_name = f"{table}_{current_datetime}"
    
    run_sql = f"""
    COPY INTO @{stage}/{csv_file_name}.csv 
    FROM (SELECT {cols} 
    FROM {database}.{schema}.{table}) 
    FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' 
    """

    # Append additional extract criteria depending on the table size
    size_sql = f"""
    SELECT table_name, row_count, trunc( (bytes/1024)/1024, 2) as MB
    FROM {database}.information_schema.tables
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    size_info_df = session.sql(size_sql).to_pandas()

    if not size_info_df.empty:
        # Check if 'MB' is lessthan 100 and allow single table
        if size_info_df['MB'].iloc[0] > 100:
            run_sql += " ) header=true "
        else:
            run_sql += " COMPRESSION=NONE) MAX_FILE_SIZE=1000000000 SINGLE=TRUE header=true"            
    else:
        st.warning("No table information available for the selected table.")

    st.code(run_sql, language='sql')

    if st.button("EXPORT", type="primary"):
        session.sql(run_sql)

        # List files in the specified stage
        list_sql = f"LIST @{stage}"
        st.code( list_sql, language='sql')
        file_list = session.sql(list_sql).collect()

        # Convert the list of Row objects to a Pandas DataFrame
        df = pd.DataFrame(file_list)
        st.write("File List in Stage:")
        st.dataframe(df)

        with st.expander("See Downloadable staged files..."):
            dl_url = f"""select GET_PRESIGNED_URL( @{stage}, '{csv_file_name}.csv') as DL_LINK;"""
            st.code(dl_url, language='sql')
            links = session.sql(dl_url).to_pandas()
            # Display the list of files as links
            if file_list:
                st.subheader("List of Files:")
                for file_info in file_list:
                    file_name = file_info[0]
                    st.markdown(f"[{file_name}]({stage}/{file_name})")

def main():
    st.title("Export CSV to STAGE")
    st.session_state.debug = True
    
    session = get_active_session()
    # session.use_secondary_roles('DATA_GOVERNOR')
    
    # Choose a database
    chosen_db = st.selectbox(
        'Select a Database:',
        ('DATA_GOVERNANCE', 'PLAYGROUND', 'EDP_BRONZE_DEV'),
        index=0
    )

    if chosen_db:
        # Load all schema/table combinations into a DataFrame
        run_sql = f"""
        select table_schema, table_name, row_count, trunc( (bytes/1024)/1024,2) as MB
        FROM {chosen_db}.information_schema.tables
        """
        table_info_df = session.sql(run_sql).to_pandas()

        chosen_schema = st.selectbox('Select Schema:', table_info_df['TABLE_SCHEMA'].unique())

        # Streamlit form for schema/table selection
        with st.form("schema_table_form"):
            # Filter tables based on the chosen schema
            filtered_tables = sorted( table_info_df[table_info_df['TABLE_SCHEMA'] == chosen_schema]['TABLE_NAME'])
            chosen_table = st.selectbox('Select Table:', filtered_tables)

            submit_button = st.form_submit_button("Generate Columns")
        if submit_button:
            # Generate column SQL only after form submission
            cols = generate_col_sql(session, chosen_db, chosen_schema, chosen_table)
            # Store the selected options in session_state
            st.session_state['selected_db'] = chosen_db
            st.session_state['selected_schema'] = chosen_schema
            st.session_state['selected_table'] = chosen_table
            st.session_state['columns'] = cols

            # Display table information
            display_table_info(session, chosen_db, chosen_schema, chosen_table)

    # Export section
    if 'columns' in st.session_state and st.session_state['columns']:
        st.subheader(f"Ready to Export {st.session_state['selected_schema']}.{st.session_state['selected_table']}")
        stage = st.text_input("Enter Stage:", "data_governance.public.unload_to_azure")
        if stage:
            export_table_to_csv(session, st.session_state['columns'], st.session_state['selected_db'], st.session_state['selected_schema'], st.session_state['selected_table'], stage)

if __name__ == '__main__':
    key_states = ['debug', 'enabled', 'has_table', 'table', 'selected_db', 'selected_schema', 'selected_table', 'columns']
    for key in key_states:
        if key not in st.session_state:
            st.session_state[key] = ""
    
    main()