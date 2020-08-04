##Получение данных по апи из яндекс директ с помощью Python

Для работы с апи необходимо получить API ключ

Подробнее о [Доступе к API](https://yandex.ru/dev/direct/doc/start/register-docpage/)

#### Поля для отчета задаются:
```python
 "FieldNames": [
                "Date",
                "CampaignName",
                "CampaignId",
                "Device",
                "Impressions",
                "Clicks",
                "Cost",
```
подробнее о допустимых полях и типах отчетов можно прочитать [в справке](https://yandex.ru/dev/direct/doc/reports/fields-list-docpage/)

Определение логинов и токена для отчета:
```python

# токены
print('Определение токена')
mytoken = 'XXXXXXXXXXXXXXXXXXXXXXXX'

# логины
print('определение логинов')
project = ['login1', 'login2', ]
print(project)
```
выбор произвольных дат отчета:
```python
start_dates = '2020-08-01'
end_dates = '2020-08-02'
```
данные за последние 3 дня (для автоматического запуска)
```python
start_dates = lastdayb
end_dates = lastday
```

#### Предобработка жанных:
добавляем НДС и переводим валюту из условных единиц в  рубли
```python
itog['Cost'] = itog['Cost'] * 1.2
itog['Cost'] = itog['Cost'] / 1000000
```

добавляем доп столбцы и исправляем названия кампаний:
```python
itog['источник'] = "yandex"
itog['поиск/сеть'] = np.where(itog['CampaignName'].str.contains("srch", case=False, na=False), 'поиск',
                              np.where(itog['CampaignName'].str.contains("-srch-cat-nz-net_", case=False, na=False),
                                       'поиск', 'сеть'))
itog['CampaignName'] = itog['CampaignName'].replace(to_replace='_10', value='', regex=True)
```

#### Очистка актуализируемых данных и перезапись

актуально для автоматического запуска отчета по расписанию

```python


print('очистка актуализируемых данных')
fix = pd.read_csv('cashe_new.csv', sep=';', encoding='cp1251', header=0)
# fix = fix[fix.Date.str.contains(r'2019|2020', case=False)==False]
fix = fix[fix.Date.str.contains(lastday) == False]
fix = fix[fix.Date.str.contains(lastdaya) == False]
fix = fix[fix.Date.str.contains(lastdayb) == False]
fix.to_csv('cashe_new.csv', index=False, header=True, sep=';', encoding='cp1251')

print('перезапись данных в итоговый файл')
x1 = pd.read_csv('cashe_new.csv', sep=';', encoding='cp1251', header=0)
x2 = pd.read_csv('cashe_next.csv', sep=';', encoding='cp1251', header=0)
x1 = x2.append(x1, ignore_index=False)
x1.to_csv('cashe_new.csv', index=False, header=True, sep=';', encoding='cp1251')

```