import pandas as pd
import numpy as np
import sqlite3

conn = sqlite3.connect('test_tran.db')

cur = conn.cursor()

# Запрос к таблице tran (клиенты, которые в месяц осуществляют по карте траты на сумму не менее 100 тыс. рублей/
# для каждой строки получение предыдущего месяца)

cur.execute("SELECT month, id, summary, lag(month, 1, 1)"
            "OVER (partition by id order by id, month) "
            "FROM (SELECT strftime('%m',day) as month, "
            "id_client id, sum(tran_sum) as summary FROM tran group by strftime('%m',day), id_client "
            "ORDER BY id_client, strftime('%m',day))"
            "WHERE summary >= 100000")

tran_sum_list = cur.fetchall()
cashback_month = []
# Начисление кэшбэка по программе
for rowid, i in enumerate(tran_sum_list):
    # Проверка, что клиент следующей и текущей строки в запросе это один клиент
    # В cashback_month добавятся строки из запроса со значениями month и id,
    # которые удовлетворяют всем условиям программы (для которых далее будет начислен кэшбэк)
    if rowid != len(tran_sum_list)-1 and (tran_sum_list[rowid][1] == tran_sum_list[rowid+1][1] or tran_sum_list[rowid][0] == '12')\
            and int(tran_sum_list[rowid][0])-1 == int(tran_sum_list[rowid][3]) and int(tran_sum_list[rowid][0]) >= 3:
        cashback_month.append(i)

for rowid, i in enumerate(cashback_month):
    if rowid != len(cashback_month)-1 and cashback_month[rowid][1] == cashback_month[rowid+1][1] \
       and int(cashback_month[rowid][0])+1 == int(cashback_month[rowid+1][0]):
        cashback_month.pop(rowid+1)

cashback_df = pd.DataFrame(cashback_month)
cashback_df.columns = ['Месяц', 'id', 'sum', 'prev_month']
cashback_df = cashback_df.drop(['prev_month', 'sum'], axis=1)
cashback_df['cashback'] = 1000

'Итого выплаты по программе кэшбэка, тыс. руб.'
cashback_summary = cashback_df[['Месяц', 'cashback']]
cashback_summary = cashback_summary.groupby(cashback_summary['Месяц']).sum()
cashback_summary['cashback'] = cashback_summary['cashback']/1000
cashback_summary.columns = ['Итого выплаты по программе кэшбэка, тыс. руб.']
df_temp3 = cashback_summary['Итого выплаты по программе кэшбэка, тыс. руб.']
cashback_summary = df_temp3.reset_index()

'Кол-во клиентов, получивших выплаты'
df_temp = cashback_df.groupby(['Месяц', 'cashback']).count()
df_temp.columns = ['Кол-во клиентов, получивших выплаты']
df_temp2 = df_temp['Кол-во клиентов, получивших выплаты']
clients_count = df_temp2.reset_index()
clients_count = clients_count.drop(['cashback'], axis=1)

'Программа выплат - Отчет'
cashback_program = cashback_summary.merge(clients_count, how='inner', on='Месяц')
cashback_program = cashback_program.set_index('Месяц')
cashback_program.index.name = None
cashback_program['Месяц'] = cashback_program.index
cashback_program = cashback_program[['Месяц', 'Итого выплаты по программе кэшбэка, тыс. руб.',
                                     'Кол-во клиентов, получивших выплаты']]

new_index = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
cashback_program = cashback_program.reindex(new_index)
cashback_program = cashback_program.replace(np.nan, 0)
cashback_program['Месяц'].update(pd.Series(["Январь", "Февраль",
                                            "Март", "Апрель",
                                            "Май", "Июнь",
                                            "Июль", "Август",
                                            "Сентябрь", "Октябрь",
                                            "Ноябрь", "Декабрь"], index=['01', '02', '03', '04', '05', '06', '07',
                                                                         '08', '09', '10', '11', '12']))

cashback_program.to_excel("cashback_program.xlsx", sheet_name='Программа лояльности', index=False)
