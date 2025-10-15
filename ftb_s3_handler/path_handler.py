from pathlib import Path

def get_parts(original_path: str):
    p = Path(original_path)

    file_name = p.parts[-1]
    full_date = '/'.join(p.parts[-4:-1])
    table_name = p.parts[1]
    schema_name = p.parts[0]

    if table_name.isdigit():
        table_name = schema_name

    return file_name, full_date, table_name, schema_name

def retrieve_new_path(original_path: str):
    file_name, full_date, table_name, schema_name = get_parts(original_path)
    return f'{schema_name}/{table_name}/{full_date}/{file_name}'

if __name__ == '__main__':

    paths = [
        "appcues/2025/1/1/2025-01-01_07-00-09.csv",
        "appcues/coiso/2025/1/1/2025-01-01_07-00-09.csv",
        "api-project-1033684201634/analytics_153835980/events/2023/03/08/20230307_events_000000000031.parquet",
        "airship/channels/2025/04/15/channels.parquet"
    ]

    for path in paths:
        print(retrieve_new_path(path))
