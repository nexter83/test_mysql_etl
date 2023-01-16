from contextlib import closing
from lib.csv_util import write_csv, read_csv
from lib.utils import make_config, transliterate_string, remove_file
import lib.db as db
from datetime import date


if __name__ == "__main__":
    config = make_config("config.json")

    for table_name in ("Таблица 1", "Таблица 2"):
        with closing(db.get_conn(config, "source_mysql_base")) as conn:
            source_count_rows = db.get_count_rows(conn, table_name)
            data = db.get_data_from_table(conn, table_name)
            list_table_information = db.get_table_information(conn, table_name)
            for i in range(len(list_table_information)):
                list_table_information[i]["translate_field"] = transliterate_string(list_table_information[i]["Field"])

            new_table_name = transliterate_string(table_name)
            new_columns = [row.get("translate_field") for row in list_table_information]
            file_name = f"{new_table_name}_{date.today()}.csv"
            write_csv(
                file_name=file_name,
                columns=new_columns,
                data=data
            )

        with closing(db.get_conn(config, "BigData_raw_tmp_mysql_base")) as tmp_conn:
            db.drop_table(tmp_conn, new_table_name)
            db.create_table(tmp_conn, new_table_name, list_table_information)
            csv_data = read_csv(file_name)
            db.upload_csv_db(tmp_conn, csv_data, new_table_name, new_columns)
            target_count_rows = db.get_count_rows(tmp_conn, new_table_name)

        if source_count_rows == target_count_rows:
            remove_file(file_name)
            with closing(db.get_conn(config, "BigData_raw_mysql_base")) as conn:
                if db.is_table_exists(conn, new_table_name):
                    db.drop_table(conn, new_table_name)
                db.copy_table(conn, new_table_name, "BigData_raw_tmp")
        else:
            print(f"Количество строк в исходной {table_name} и целевой {new_table_name} таблицах - не совпадают")
