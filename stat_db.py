import psycopg2
import datetime
import pandas as pd
import numpy as np

try:
    # пытаемся подключиться к базе данных
    con = psycopg2.connect(dbname='itcoty', user='postgres', password='admin', host='localhost', )
    print('connected')
except:
    # в случае сбоя подключения будет выведено сообщение в STDOUT
    print('Can`t establish connection to database')

# Для внесения статистики по уже имеющимся вакансиям: 2 функции-создаем таблицу и отправляем туда опубликованные вакансии. Затем, для новых вакансий - нужно добавить функцию push_vacancy_to_main_stats(dict, table_name) в готовую функцию push_to_db_write_message

def add_column_into_table(table_name, column_name):
    """"добавляю столбец с новым сабом в таблицу для статистики"""
    cur = con.cursor()
    with con:
        query = f"""ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0"""
        cur.execute(query)
    print(f'Column {column_name} to {table_name} has been added or exists')

def check_or_create_stats_table(table_name, profession_list=[]):
    """ Создаем таблицу для статистики"""
    if not profession_list:
        profession_list=['backend', 'frontend']

    cur = con.cursor()
    with con:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                            time_of_public TIMESTAMP,
                            chat_name VARCHAR(150)
                            );"""
                        )
    for i in profession_list:
        list=[f'{i}_all', f'{i}_unique']
        for j in list:
            add_column_into_table(table_name, j)

    print(f'table {table_name} has been created or exists')

def decompose_from_str_to_subs_list(data_str):
    """Парсим данные по sub"""
    data_list=data_str.split(': ')
    profession=data_list[0]
    i=data_list[1].strip()
    if i=='':
        i='unsorted'
    i=i.split(', ')
    subs_list=[f'{profession}_{j}' for j in i]
    return subs_list

def push_vacancy_to_main_stats(dict, table_name):
    """Добавляем вакансию в  таблицу статистики"""

    time_of_public = dict['time_of_public']
    chat_name = dict['chat_name']
    subs_list=decompose_from_str_to_subs_list(dict['sub'])
    profession = dict['profession']
    all=f'{profession}_all'
    unique=f'{profession}_unique'

    cur = con.cursor()
    for sub in subs_list:
        add_column_into_table(table_name, sub)
        # add_column_into_table(table_name, all) - можно добавить если предполагается, что профессии тоже могут появиться новые
        # add_column_into_table(table_name, unique)
        query = f"""SELECT * FROM {table_name} WHERE time_of_public='{time_of_public}' AND chat_name='{chat_name}'"""
        cur.execute(query)

        if not cur.fetchall():
            query = f"""INSERT INTO {table_name} (chat_name, time_of_public, {sub}, {all}) VALUES ('{chat_name}','{time_of_public}','1', '1')"""
            with con:
                try:
                    cur.execute(query)
                    print("Vacancy was added to subs_stats")
                except Exception as e:
                    print('error', e)

        else:
            query = f"""UPDATE {table_name} SET {sub} = {sub}+1, {all} = {all}+1 WHERE chat_name = '{chat_name}' AND time_of_public = '{time_of_public}'"""
            with con:
                try:
                    cur.execute(query)
                    print(f"Vacancy was added to subs_stats")
                except Exception as e:
                    print('error', e)

    query = f"""UPDATE {table_name} SET {unique}={unique}+1 WHERE chat_name = '{chat_name}' AND time_of_public = '{time_of_public}'"""
    with con:
        try:
            cur.execute(query)
            print("Vacancy was added to subs_stats")
        except Exception as e:
            print('error', e)

    return dict

def get_all_from_stat_db(table_name, param='', order=None, field='*'):
    """Получаем данные из стат.таблицы для отчета"""

    cur = con.cursor()

    if not order:
        order = "ORDER BY time_of_public"

    query = f"""SELECT {field} FROM {table_name} {param} {order}"""
    with con:
        try:
            cur.execute(query)
            response = cur.fetchall()
            column_names = [description[0] for description in cur.description]
        except Exception as e:
            print(e)
            return str(e)

    return response, column_names

def make_report(table_name, date1, date2):
    param=f"WHERE DATE(time_of_public) BETWEEN '{date1}' AND '{date2}'"
    response = get_all_from_stat_db(param=param, table_name=table_name)

    return response


# print(make_report('main_stats', '2023-01-02', '2023-01-04'))

def make_report_excel(table_name, date1, date2):
    """выводит отчет в excell. Даты вводить в формате: '2023-01-02'"""

    param=f"WHERE DATE(time_of_public) BETWEEN '{date1}' AND '{date2}'"
    response = get_all_from_stat_db(param=param, table_name=table_name)
    columns=response[1]
    all=[i for i in columns if 'all' in i]
    unique=[i for i in columns if 'unique' in i]
    df=pd.DataFrame(response[0], columns=columns)
    df['time_of_public'] = df['time_of_public'].dt.date
    df=df.set_index(['time_of_public', 'chat_name'])
    df['Unique']=df[unique].sum(axis=1)
    df['All']=df[all].sum(axis=1)
    df = df[sorted(df.columns )]
    df=df.fillna("")
    df2=pd.pivot_table(df, index=['chat_name'], values=df, aggfunc=np.sum)
    df2.loc['Total for period']=df2.sum(axis=0, numeric_only=True)
    df_new=pd.concat([y.append(y.sum().rename((x, 'Total for day'))) for x, y in df.groupby (level= 0)]).append(df.sum().rename((f'{date1}-{date2}', 'Total for period')))
    len=df_new.shape[0]

    with pd.ExcelWriter("statistics.xlsx") as writer:
        df_new.to_excel(writer, sheet_name="Sheet1")
        df2.to_excel(writer, sheet_name="Sheet1", startrow=len+2,startcol=1, header=False)
    print('Report is done, saved')

def add_old_vacancies_to_stat_db(table_list=None, fields=None, table_name=None):

    fields='time_of_public, chat_name, profession, sub'
    for i in table_list:
        response=get_all_from_db(table_name=i, field=fields)
        for i in response:
            result_dict=helper.to_dict_from_admin_response_sync(i, fields)
            push_vacancy_to_main_stats(result_dict, table_name)
