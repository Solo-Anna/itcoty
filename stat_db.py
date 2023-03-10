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

def add_column_into_table(column_name, table_name=None):
    """"добавляю столбец с новым сабом в таблицу для статистики"""
    if not table_name:
        table_name='stats_db'
    cur = con.cursor()
    with con:
        query = f"""ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0"""
        cur.execute(query)
    print(f'Column {column_name} to {table_name} has been added or exists')

def check_or_create_stats_table(table_name=None, profession_list=[]):
    """ Создаем таблицу для статистики"""
    if not table_name:
        table_name='stats_db'
    if not profession_list:
        profession_list=['designer', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'marketing', 'devops', 'hr', 'backend', 'qa', 'junior']

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
            add_column_into_table(j, table_name)

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

def push_vacancy_to_main_stats(dict, table_name=None):
    """Добавляем вакансию в  таблицу статистики"""
    if not table_name:
        table_name='stats_db'
    time_of_public = dict['time_of_public']
    chat_name = dict['chat_name']
    subs_list=decompose_from_str_to_subs_list(dict['sub'])
    profession = dict['profession']
    all=f'{profession}_all'
    unique=f'{profession}_unique'

    cur = con.cursor()
    for sub in subs_list:
        add_column_into_table(sub, table_name)
        # add_column_into_table(all, table_name) - можно добавить если предполагается, что профессии тоже могут появиться новые
        # add_column_into_table(unique, table_name)
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

def get_all_from_stat_db(table_name=None, param='', order=None, field='*'):
    """Получаем данные из стат.таблицы для отчета"""

    if not table_name:
        table_name='stats_db'

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

    return {'response':response, 'column_names':column_names}

def make_report_excel(date1, date2, table_name=None):
    """выводит отчет в excell. Даты вводить в формате: '2023-01-02'"""

    if not table_name:
        table_name='stats_db'

    param=f"WHERE DATE(time_of_public) BETWEEN '{date1}' AND '{date2}'"
    data = get_all_from_stat_db(param=param, table_name=table_name)
    columns=data['column_names']
    all=[i for i in columns if 'all' in i]
    unique=[i for i in columns if 'unique' in i]
    df=pd.DataFrame(data['response'], columns=columns)
    df['time_of_public'] = df['time_of_public'].dt.date
    df=df.set_index(['time_of_public', 'chat_name'])
    df['Unique']=df[unique].sum(axis=1)
    df['All']=df[all].sum(axis=1)
    df = df[sorted(df.columns )]
    df2=pd.pivot_table(df, index=['chat_name'], values=df, aggfunc=np.sum)
    df2.loc['Total for period']=df2.sum(axis=0, numeric_only=True)
    df_new=pd.concat([y.append(y.sum().rename((x, 'Total for day'))) for x, y in df.groupby (level= 0)]).append(df.sum().rename((f'{date1}-{date2}', 'Total for period')))
    len=df_new.shape[0]

    with pd.ExcelWriter("statistics.xlsx") as writer:
        df_new.to_excel(writer, sheet_name="Sheet1")
        df2.to_excel(writer, sheet_name="Sheet1", startrow=len+2,startcol=1, header=False)
    print('Report is done, saved')

def get_all_from_db(table_name, param='', without_sort=False, order=None, field='*', curs=None):
    """from db_operations.scraping_db import DataBaseOperations"""

    cur = con.cursor()

    if not order:
        order = "ORDER BY time_of_public"

    if not without_sort:
        query = f"""SELECT {field} FROM {table_name} {param} {order}"""
    else:
        query = f"""SELECT {field} FROM {table_name} {param} """

    with con:
        try:
            cur.execute(query)
            response = cur.fetchall()
        except Exception as e:
            print(e)
            return str(e)
    if curs:
        return cur
    return response

def to_dict_from_admin_response_sync(response, fields):
    """from helper_functions.helper_functions import to_dict_from_admin_response_sync"""

    response_dict = {}
    fields = fields.split(', ')
    for i in range(0, len(fields)):
        response_dict[fields[i]] = response[i]
    return response_dict

def add_old_vacancies_to_stat_db(table_list=None, fields=None, table_name=None):

    if not table_list:
        table_list=['designer', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'marketing', 'devops', 'hr', 'backend', 'qa', 'junior']
    fields='time_of_public, chat_name, profession, sub'
    for i in table_list:
        response=get_all_from_db(table_name=i, field=fields)
        for i in response:
            result_dict=to_dict_from_admin_response_sync(i, fields)
            push_vacancy_to_main_stats(result_dict, table_name)
        print(f'All vacancies from {i} were added to stats db')


# table_list=['vacancies']
# check_or_create_stats_table('stats_db')
# add_old_vacancies_to_stat_db(table_list=table_list)
make_report_excel('2023-01-02', '2023-01-04')
