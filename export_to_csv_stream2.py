# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

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
    run_sql = f"""
    COPY INTO @{stage}/{table}.csv 
    FROM (SELECT {cols} 
    FROM {database}.{schema}.{table}) 
    FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION=NONE) SINGLE=TRUE header=true
    """

    st.code(run_sql, language='sql')

    if st.button("EXPORT", type="primary"):
        session.sql(run_sql)
        st.success("Export Complete")

def main():
    st.title("Export CSV to STAGE")
    st.session_state.debug = True
    
    session = get_active_session()

    # Choose a database
    chosen_db = st.selectbox(
        'Select a Database:',
        ('DATA_GOVERNANCE', 'PLAYGROUND', 'EDP_BRONZE_DEV'),
        index=0
    )

    if chosen_db:
        # Load all schema/table combinations into a DataFrame
        run_sql = f"""
        SELECT table_schema, table_name 
        FROM {chosen_db}.information_schema.tables
        """
        table_info_df = session.sql(run_sql).to_pandas()

        # Streamlit form for schema/table selection
        with st.form("schema_table_form"):
            chosen_schema = st.selectbox('Select Schema:', table_info_df['table_schema'].unique())
            # Filter tables based on the chosen schema
            filtered_tables = table_info_df[table_info_df['table_schema'] == chosen_schema]['table_name']
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
