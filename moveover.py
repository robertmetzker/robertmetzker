while True:
    records = cur.fetchmany(batch_size)
    if not records:
        break

    # Convert records to a DataFrame
    records = [tuple(record) for record in records]  # Ensure records are in the correct format
    columns = [desc[0] for desc in cur.description]  # Extract column names from cursor description
    df = pd.DataFrame(records, columns=columns)
    total_row_count += len(df)  # Update row count
    
    # Write DataFrame to CSV file
    mode = 'a' if os.path.exists(csv_file_path) else 'w'
    header = not os.path.exists(csv_file_path)
    df.to_csv(csv_file_path, mode=mode, header=header, index=False, sep='~', doublequote=True, escapechar='\\')

    if first_batch:
        headers = df.columns.tolist()  # Capture headers from the first batch
        first_batch = False