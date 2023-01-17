from typing import Union
from sqlalchemy import create_engine, inspect, MetaData, Table
import time


def get_data_from_table(conn, table_name: str, columns: Union[list[str], None] = None) -> list[tuple]:
    """
    Получаем данные из таблицы БД
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы
    :param columns: список столбцов
    :return: Список кортежей с данными
    """
    sql = create_sql_query(table_name, columns)
    return get_records(conn, sql)


def get_table_information(conn, table_name: str) -> list[dict]:
    """
    Возвращает информацию о колонках и их типе в базе данных
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы
    :return: список словарей
    """
    return get_dicts(conn, f"DESCRIBE `{table_name}`")


def get_count_rows(conn, table_name: str) -> int:
    """
    Получаем количество строк в таблице
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы
    :return: число
    """
    return get_records(conn, f"select count(*) from `{table_name}`;")[0]


def create_table(conn, table_name: str, table_information: list[dict]):
    """
    Создаем таблицу в базе
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы
    :param table_information: данные с информацией о столбцах полученные функцией get_table_information
    :return: None
    """
    columns_data = ",".join(
        [
            f"{row['translate_field']} {row['Type']}"
            for row in table_information
        ]
    )
    sql = "create table %s (%s);"
    conn.execute(sql % (table_name, columns_data))


def upload_csv_db(conn, csv_data: list[tuple], table_name: str, columns: list[str]):
    """
    Загружает данные из csv в базу
    :param conn: sqlalchemy подключение
    :param csv_data: данные загруженные из csv функцией read_csv
    :param table_name: имя таблицы в базе
    :param columns: список столбцов
    :return: None
    """
    cols = ",".join(columns)
    sql = f"INSERT INTO %s(%s) VALUES({','.join(['%%s' for _ in range(len(columns))])})" % (table_name, cols)
    sql = sql.format(table_name)
    conn.execute(sql, csv_data[1:])


def is_table_exists(conn, table_name: str) -> bool:
    """
    Проверяет существует ли таблица в базе
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы в базе
    :return: True or False
    """
    insp = inspect(conn)
    return insp.has_table(table_name)


def create_connection_string(host: str, db_name: str, user_name: str, user_pass: str) -> str:
    """
    Возвращает строку подключения к базе данных
    :param host:
    :param db_name:
    :param user_name:
    :param user_pass:
    :return:
    """
    return f"mysql+pymysql://{user_name}:{user_pass}@{host}/{db_name}"


def get_engine(config: dict, db: str):
    """Возвращает sqlalchemy engine для коннекта к базе """
    connection_string = create_connection_string(
        host=config[db]["host"],
        db_name=config[db]["db_name"],
        user_name=config[db]["user_name"],
        user_pass=config[db]["user_pass"]
    )
    return create_engine(connection_string)


def get_conn(config: dict, db: str):
    """Возвращает подключение sqlalchemy к базе"""
    return get_engine(config, db).connect()


def get_dicts(conn, sql: str) -> list[dict]:
    """Возвращает результат запроса к базе в виде списка словарей, ключом является имя столбца"""
    data = conn.execute(sql)
    columns = [column[0] for column in data.cursor.description]
    data = data.fetchall()
    return [
        dict(zip(columns, row))
        for row in data
    ]


def get_records(conn, sql: str):
    """Возвращает результат запроса к базе в виде списка кортежей, без данных о столбцах"""
    data = conn.execute(sql)
    return data.fetchall()


def create_sql_query(table_name: str, columns: Union[list[str], None] = None):
    """Для упрощения создания запроса на выбор значений таблицы"""
    if not columns:
        return f"select * from `{table_name}`;"
    return f"select {','.join(columns)} from `{table_name}`;"


def copy_table(conn, table_name: str, source_schema: str):
    """
    Копируем таблицу из другой схемы в целевую
    :param conn: sqlalchemy подключение к целевой таблице
    :param table_name: Имя таблицы
    :param source_schema: Схема откуда копируем таблицу
    :return:
    """
    conn.execute("create table %s select * from %s.%s" % (table_name, source_schema, table_name))


def drop_table(conn, table_name: str):
    """
    Удаляет таблицу в базе
    :param conn: sqlalchemy подключение
    :param table_name: имя таблицы в базе
    :return:
    """
    conn.execute("drop table if exists %s;" % table_name)


def move_table(conn, table_name: str, source_schema: str):
    conn.execution_options(isolation_level="SERIALIZABLE")
    with conn.begin():
        if is_table_exists(conn, table_name):
            drop_table(conn, table_name)
        # time.sleep(15)
        copy_table(conn, table_name, source_schema)