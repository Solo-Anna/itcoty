import datetime
from collections import Counter

def decompose_from_str_to_list(data_str):
    """To import this function from helper_functions"""

    data_dict = {}
    data_list = data_str.split('; ')
    for i in data_list:
        i = i.split(': ')
        key = i[0]
        sub_items = i[1]
        if sub_items:
            data_dict[key] = sub_items.split(', ')
        else:
            data_dict[key] = []
    return data_dict


def stats_one_table_one_day(self, table_name, date):
    """Returns statistics for one profession for one day."""

    if not date:
        date=datetime.date.today()
        param=f"WHERE DATE(time_of_public)='{date}'"

    fields="chat_name, sub"
    response = self.get_all_from_db(param=param, table_name=table_name, field=fields)
    stats= {'date': date, 'profession': table_name}
    for i in response:
        pros = i['sub']
        channel = i['chat_name']
        data_dict=decompose_from_str_to_list(pros)
        subs_list=list(data_dict.values())[0]
        if not channel in stats:
            stats[channel]={'all':0, 'unique':0, 'unsorted':0}
        stats[channel]['unique']+=1
        if subs_list==[]:
            stats[channel]['unsorted']+=1
            stats[channel]['all']+=1
        else:
            for s in subs_list:
                if not s in stats[channel]:
                    stats[channel][s]=0
                stats[channel][s]+=1
                stats[channel]['all']+=1
        count=Counter()
    for v in stats.values():
        count.update(v)
    print(dict(count))
    stats['Summary']=dict(count)

    return stats
