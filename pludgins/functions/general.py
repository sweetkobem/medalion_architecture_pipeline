def get_filtered_column(key_destination, columns):
    return list(
        map(lambda x: f"COALESCE({x}::TEXT, '')",
            filter(lambda x: x not in [key_destination, 'start_date', 'end_date', 'is_active'],
                   columns))
    )


def encoded_column_by_sql(filtered_column):
    return f"MD5({'''||'::'||'''.join(filtered_column)}) AS encoded"


def schema_dict_to_sql(schema, exclude_keys=[]):
    if exclude_keys:
        schema = {k: v for k, v in schema.items() if k not in exclude_keys}

    sql = [f"{k} {v}" for k, v in schema.items()]

    return ",".join(sql)


def is_table_exists(conn, table):
    table_exists = conn.execute(
        f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table}'
        """).fetchone()[0]

    if table_exists > 0:
        return True
    else:
        return False


def get_last_date(conn, table):
    return conn.execute(
        f"""
        SELECT
            MAX(last_date)::VARCHAR AS last_date
        FROM (
        SELECT MAX(start_date) AS last_date
        FROM {table}
        WHERE is_active IS TRUE
        UNION ALL
        SELECT MAX(end_date) AS last_date
        FROM {table}
        WHERE is_active IS TRUE
        )""").fetchone()[0]


def get_total_data_in_execution_date(conn, table, column, execution_date):
    return conn.execute(
        f"""
        SELECT
            COUNT(1) AS total
        FROM {table}
        WHERE {column}::DATE = '{execution_date}'
        """).fetchone()[0]


def dim_table_get_changes_data(
        conn, execution_date, table, key_destination, key_source, md5_column):
    return conn.execute(f"""
        SELECT
            data.*,
            '{execution_date}' AS start_date,
            NULL AS end_date,
            TRUE AS is_active,
            source.{key_destination}
        FROM (
            SELECT
                *, {md5_column}
            FROM temp_{table}
        ) data
        LEFT JOIN (
            SELECT
                *, {md5_column}
            FROM {table}
            WHERE is_active IS TRUE
        ) source
        ON (data.{key_source}=source.{key_source}
            AND source.encoded=data.encoded)
        WHERE source.encoded IS NULL
        """).fetchdf()


def dim_table_store_data(
        conn, execution_date, table, key_destination, columns, data):
    return conn.execute(f"""
            UPDATE {table}
            SET end_date = '{execution_date}', is_active = FALSE
            WHERE {key_destination} IN (
                SELECT {key_destination} FROM data
                WHERE {key_destination} IS NOT NULL
            )
            AND is_active = TRUE;

            INSERT INTO {table} ({','.join(columns)})
            SELECT {','.join(columns)}
            FROM data WHERE {key_destination} IS NULL;
            """)


def fact_table_store_data(
        conn, table, columns, data):
    return conn.execute(f"""
            INSERT INTO {table} ({','.join(columns)})
            SELECT {','.join(columns)} FROM data;
            """)
