#!/usr/bin/env python
# coding: utf-8

print('импорт библиотек')
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import datetime
from datetime import datetime as dt
from datetime import date, timedelta
import time

import pandas as pd
import numpy as np
from pandas import Series, DataFrame
import datetime
from datetime import date, timedelta

print("формирование данных для отчета")
# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import sys

datename = datetime.datetime.now()


def rep(token, login, date_from, date_to):
    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x

    # --- Входные данные ---
    # Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

    # OAuth-токен пользователя, от имени которого будут выполняться запросы
    token = token

    # Логин клиента рекламного агентства
    # Обязательный параметр, если запросы выполняются от имени рекламного агентства
    clientLogin = login

    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
        # OAuth-токен. Использование слова Bearer обязательно
        "Authorization": "Bearer " + token,
        # Логин клиента рекламного агентства
        "Client-Login": clientLogin,
        # Язык ответных сообщений
        "Accept-Language": "ru",
        # Режим формирования отчета
        "processingMode": "auto"
        # Формат денежных значений в отчете
        # "returnMoneyInMicros": "false",
        # Не выводить в отчете строку с названием отчета и диапазоном дат
        # "skipReportHeader": "true",
        # Не выводить в отчете строку с названиями полей
        # "skipColumnHeader": "true",
        # Не выводить в отчете строку с количеством строк статистики
        # "skipReportSummary": "true"
    }

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignName",
                "CampaignId",
                "Device",
                "Impressions",
                "Clicks",
                "Cost",

            ],
            "ReportName": u('report'),
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    # Кодирование тела запроса в JSON
    body = json.dumps(body, indent=4)

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 200:

                format(u(req.text))
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print(
                    "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break

    json_string = json.dumps(body)
    return req.text


#определение дат

lastday = datetime.datetime.now()
lastday = lastday - timedelta(days=1)
lastday = lastday.strftime("%Y-%m-%d")

lastdaya = datetime.datetime.now()
lastdaya = lastdaya - timedelta(days=2)
lastdaya = lastdaya.strftime("%Y-%m-%d")

lastdayb = datetime.datetime.now()
lastdayb = lastdayb - timedelta(days=3)
lastdayb = lastdayb.strftime("%Y-%m-%d")

# Функции вывода Датафрейма
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)

# токены
print('Определение токена')
mytoken = 'XXXXXXXXXXXXXXXXXXXXXXXX'

# логины
print('определение логинов')
project = ['login1', 'login2', ]
print(project)

# Выбор дат:
DateFrom='2020-08-01'
DateTo='2020-08-02'
#DateFrom = lastdayb
#DateTo = lastday

# # Сбор статистики
itog = pd.DataFrame()
x = 0
start_time = dt.now()
print('выгрузка данных:')
print(DateFrom)
print(DateTo)
while x <= len(project) - 1:
    try:
        print(x)
        print(project[x])
        prjct = project[x]
        data = rep(mytoken, prjct, DateFrom, DateTo)
        file = open("cashe.csv", "w")
        file.write(data)
        file.close()
        f = pd.read_csv('cashe.csv', sep='	', encoding='cp1251', header=1)
        f['акаунт'] = project[x]
        itog = itog.append(f, ignore_index=False)
        time.sleep(1)
        x = x + 1
    except:
        print('ошибка')
        x = x + 1

print(dt.now() - start_time)


start_time = dt.now()
print('предобработка данных')
itog['Cost'] = itog['Cost'] * 1.2
itog['Cost'] = itog['Cost'] / 1000000
itog = itog[itog.Date.str.contains('Total') == False]

itog['источник'] = "yandex"
itog['поиск/сеть'] = np.where(itog['CampaignName'].str.contains("srch", case=False, na=False), 'поиск',
                              np.where(itog['CampaignName'].str.contains("-srch-cat-nz-net_", case=False, na=False),
                                       'поиск', 'сеть'))
itog['тип'] = np.where(itog['CampaignName'].str.contains("dsa", case=False, na=False), 'dsa',
                       np.where(itog['CampaignName'].str.contains("-nz", case=False, na=False), 'nz',
                                np.where(itog['CampaignName'].str.contains("_nz", case=False, na=False), 'nz',
                                         np.where(itog['CampaignName'].str.contains("shop", case=False, na=False),
                                                  'shoping',
                                                  np.where(itog['CampaignName'].str.contains("corporate", case=False,
                                                                                             na=False), 'b2b',
                                                           np.where(
                                                               itog['CampaignName'].str.contains("promo", case=False,
                                                                                                 na=False), 'акции',
                                                               np.where(itog['CampaignName'].str.contains("brand",
                                                                                                          case=False,
                                                                                                          na=False),
                                                                        'бренд',
                                                                        np.where(
                                                                            itog['CampaignName'].str.contains("cat-cv",
                                                                                                              case=False,
                                                                                                              na=False),
                                                                            'кат + вендор',
                                                                            np.where(itog['CampaignName'].str.contains(
                                                                                "categor", case=False, na=False),
                                                                                'категории',
                                                                                np.where(itog[
                                                                                    'CampaignName'].str.contains(
                                                                                    "compet", case=False,
                                                                                    na=False), 'конкуренты',
                                                                                    np.where(itog[
                                                                                        'CampaignName'].str.contains(
                                                                                        "config", case=False,
                                                                                        na=False),
                                                                                        'конфигуратор',
                                                                                        np.where(itog[
                                                                                            'CampaignName'].str.contains(
                                                                                            "rmkt",
                                                                                            case=False,
                                                                                            na=False),
                                                                                            'ремарктеинг',
                                                                                            np.where(
                                                                                                itog[
                                                                                                    'CampaignName'].str.contains(
                                                                                                    "usilenie",
                                                                                                    case=False,
                                                                                                    na=False),
                                                                                                'усиление',
                                                                                                'разное')))))))))))))
itog['CampaignName'] = itog['CampaignName'].replace(to_replace='_10', value='', regex=True)
print('запись во временный файл поулченных данных')
itog.to_csv("cashe_next.csv", index=False, header=True, sep=";", encoding='cp1251')

print("дата: ", itog['Date'].min(), "-", itog['Date'].max())
print("количество строк: ", len(f))
print("показы", itog['Impressions'].sum())
print("клики", itog['Clicks'].sum())
print("расход", itog['Cost'].sum())
print(dt.now() - start_time)
itog.head()

'''
start_time = dt.now()
print('очистка актуализируемых данных')
fix = pd.read_csv('cashe_new.csv', sep=';', encoding='cp1251', header=0)
# fix = fix[fix.Date.str.contains(r'2019|2020', case=False)==False]
fix = fix[fix.Date.str.contains(lastday) == False]
fix = fix[fix.Date.str.contains(lastdaya) == False]
fix = fix[fix.Date.str.contains(lastdayb) == False]
fix.to_csv('cashe_new.csv', index=False, header=True, sep=';', encoding='cp1251')

print(dt.now() - start_time)


start_time = dt.now()
print('перезапись данных в итоговый файл')
x1 = pd.read_csv('cashe_new.csv', sep=';', encoding='cp1251', header=0)
x2 = pd.read_csv('cashe_next.csv', sep=';', encoding='cp1251', header=0)
x1 = x2.append(x1, ignore_index=False)
x1.to_csv('cashe_new.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
'''