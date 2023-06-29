import psycopg2.extras
import datetime
import glob
import pandas as pd
import datetime
import io
import warnings

warnings.simplefilter("ignore")

dict_for_operator = \
    {
        'Общество с ограниченной ответственностью «Скартел»': 'Скартел',
        'Общество с ограниченной ответственностью \"Скартел\"': 'Скартел',

        'Общество с ограниченной ответственностью \"Т2 Мобайл\"': 'Т2 Мобайл',
        'Общество с ограниченной ответственностью «Т2 Мобайл»': 'Т2 Мобайл',

        'Публичное акционерное общество «Мобильные ТелеСистемы»': 'МТС',
        'Публичное акционерное общество \"Мобильные ТелеСистемы\"': 'МТС',

        'Публичное акционерное общество \"МегаФон\"': 'МегаФон',
        'Публичное акционерное общество «МегаФон»': 'МегаФон',

        'Публичное акционерное общество \"Ростелеком\"': 'Ростелеком',
        'Публичное акционерное общество «Ростелеком»': 'Ростелеком',
        'Публичное акционерное общество междугородной и международной электрической связи \"Ростелеком\"': 'Ростелеком',

        'Публичное акционерное общество «Вымпел-Коммуникации»': 'ВымпелКом',
        'Публичное акционерное общество \"Вымпел-Коммуникации\"': 'ВымпелКом'
    }

dict_ETC = \
    {
        '18.1.1.3.': 'GSM',
        '18.1.1.8.': 'GSM',
        '18.1.1.5.': 'UMTS',
        '18.1.1.6.': 'UMTS',
        '18.7.1.': 'LTE',
        '18.7.4.': 'LTE',
        '18.7.5.': '5G NR',
        '19.2.': 'РРС'
    }


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time}")
        return result

    return wrapper


def convert_exel_to_csv(file):
    cur_time = datetime.datetime.now()
    time_now = f'{cur_time.day}-{cur_time.month:02}-{cur_time.year}_{cur_time.hour:02}_{cur_time.minute:02}_{cur_time.second:02}'

    file_all = pd.read_excel(file).loc[:,['Наименование РЭС', 'Адрес', '№ вида ЕТС', 'Владелец', 'Широта', 'Долгота', 'Частоты', 'Дополнительные параметры', 'Классы излучения', 'Серия последнего действующего РЗ/СоР', 'Номер последнего действующего РЗ/СоР']]

    file_all['Наименование РЭС'] = file_all['Наименование РЭС'].str.strip()
    file_all['Адрес'] = file_all['Адрес'].str.strip().str.replace('., ', ', ').str.replace('\"', '')
    file_all['№ вида ЕТС'] = [dict_ETC[x.strip()] for x in file_all['№ вида ЕТС']]
    file_all['Владелец'] = [dict_for_operator[x.strip()] for x in file_all['Владелец']]
    file_all['Широта'] = file_all['Широта'].str.strip()
    file_all['Долгота'] = file_all['Долгота'].str.strip()
    file_all['Частоты'] = file_all['Частоты'].str.strip()
    file_all['Дополнительные параметры'] = file_all['Дополнительные параметры'].str.strip()
    file_all['Классы излучения'] = file_all['Классы излучения'].str.strip()
    file_all['Серия последнего действующего РЗ/СоР'] = file_all['Серия последнего действующего РЗ/СоР'].str.strip()

    file_all.to_csv(f'source_folder\\{time_now}.csv', sep='^', index=False)

@measure_time
def write_to_postgres(file_csv):
    hostname = 'localhost'
    database = 'eirs'
    username = 'postgres'
    password = '1234'
    port_id = 5432
    connection = None

    try:
        with psycopg2.connect(host=hostname, dbname=database, user=username, password=password, port=port_id) as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                cur.execute('DROP TABLE IF EXISTS cellular')
                create_table = """ CREATE TABLE IF NOT EXISTS cellular (
                                        РЭС                       text,
                                        Адрес                     varchar(230),
                                        ТИП_РЭС                   text,
                                        Владелец                  varchar(11),
                                        Широта                    varchar(9),
                                        Долгота                   varchar(9),
                                        Частоты                   varchar(756),
                                        Дополнительные_параметры  text,
                                        Классы_излучения          varchar(53),
                                        Серия_Номер_РЗ_СоР        text)
                                """
                cur.execute(create_table)

                with io.open(file_csv, mode="r", encoding='utf-8') as csv_file:
                    data = []
                    for row in csv_file.readlines():
                        data = row.strip().split('^')

                        cur.execute("INSERT INTO cellular (РЭС, Адрес, ТИП_РЭС, Владелец, Широта, Долгота, Частоты, Дополнительные_параметры, Классы_излучения, Серия_Номер_РЗ_СоР) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                         (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], f'{data[9]} {data[10]}'))

                    data.clear()

                connection.commit()

    except Exception as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


file_xlsx = glob.glob('source_folder\*.xlsx')
convert_exel_to_csv(file_xlsx[0])

file_csv = glob.glob('source_folder\*.csv')
write_to_postgres(file_csv[0])
