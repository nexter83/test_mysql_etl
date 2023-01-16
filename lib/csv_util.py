import csv
from datetime import datetime


def write_csv(file_name: str, columns: list[str], data: list[tuple[str, datetime, int, float]]) -> None:
    """
    Записывает данные из базы в csv файл
    :param file_name: имя файла
    :param columns: список столбцов
    :param data: список с кортежами данных
    :return: None
    """
    with open(file_name, "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(data)


def read_csv(file_name: str):
    """
    Читает csv файл и отдает списком
    :param file_name: имя файла
    :return: None
    """
    with open(file_name, "r", encoding="utf-8") as file:
        rows = csv.reader(file, delimiter=",", quotechar='"')
        return list(rows)
