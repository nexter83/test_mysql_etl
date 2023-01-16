import pandas as pd
from contextlib import closing


from lib.utils import make_config
from lib.db import get_conn

if __name__ == "__main__":
    config = make_config("../config.json")
    with closing(get_conn(config)) as conn:
        init_data1 = pd.read_excel("Данные в таблицах.xlsx", sheet_name="Таблица 1")
        init_data1.to_sql("Таблица 1", conn, index=False)

        init_data2 = pd.read_excel("Данные в таблицах.xlsx", sheet_name="Таблица 2")
        init_data2.to_sql("Таблица 2", conn, index=False)
