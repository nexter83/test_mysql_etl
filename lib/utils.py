from pathlib import Path
import json
from transliterate import translit
from string import ascii_lowercase


legal_character = ascii_lowercase + ''.join(map(str, range(10)))


def make_config(path: str) -> dict:
    """
    Читаем json файл с данными для входа в БД
    :param path: Файл и путь к нему
    :return: Словарь
    """
    cfg_file = Path(path)
    if not cfg_file.exists():
        raise Exception("Config file {} doesn't exists".format(path))
    with open(path, "r") as fd:
        res = json.loads(fd.read())
    return res


def transliterate_string(string: str):
    """
    Транслит строки из кириллицы в латиницу, с заменой всех символов кроме букв и цифр на _
    :param string: Строка для перевода в латиницу
    :return:
    """
    data = translit(string, "ru", reversed=True)
    return "".join([t if t.lower() in legal_character else "_" for t in data])


def remove_file(file_name: str):
    """
    Удаляет файл если он существует
    :param file_name:
    :return:
    """
    file = Path(file_name)
    file.unlink(missing_ok=True)
