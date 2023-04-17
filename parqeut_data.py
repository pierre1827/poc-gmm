import pandas as pd
import time
import numpy as np
from datetime import datetime

start_time = datetime.today()
file_path = r'data\GMMActivity_20230320.parquet'
df = pd.read_parquet(file_path)
# print(df.columns)
# df_to_csv = df.to_csv(r'data\GMMActivity_20230320.csv',index=False, sep=',')
# print(df)
df = df[['StreamingId','UserID','StreamingDate','StreamingTime','StreamDuration']]
df['UserID'] = df['UserID'].astype(int)
df['StreamingDate'] = df['StreamingDate'].astype(str)
df['StreamingTime'] = df['StreamingTime'].astype(str)
df['StreamDuration'] = df['StreamDuration'].astype(int)
df['StreamingDateTime'] = pd.to_datetime(df['StreamingDate']+' '+df['StreamingTime'])
df = df.sort_values(by='UserID',ignore_index=True)
# df = df.sort_values(by='StreamingDateTime',ignore_index=True)

def remove_item_list(list):
    for index, item in enumerate(list):
        user_list.remove(item)
    return user_list

user_list= df['UserID'].tolist()
sum_stream = 0
result_df = pd.DataFrame()
# print(user_list.index(17450112))


for index,user_id_item in enumerate(user_list):
    print(index)
    df_user = df[df['UserID']==user_id_item]
    df_user = df_user[['StreamingId','UserID','StreamingDateTime','StreamDuration']].sort_values(by='StreamingDateTime',ignore_index=True)
    stream_list = df_user['StreamDuration'].tolist()

    sum_result_list = []
    sum_StreamDuration = 0
    for index,data in enumerate(stream_list):
        # print(data['StreamDuration'])
        sum_StreamDuration +=data
        sum_result_list.append(sum_StreamDuration)
        user_list.remove(user_id_item)

        # time.sleep(3)
        
    df_user['sum_StreamDuration'] = sum_result_list
    result_df = pd.concat([df_user,result_df])
    
        
    # user_list = remove_item_list(remove_user_list)

    # time.sleep(3)
        # print("stream2: ", df_user['StreamDuration'].shift(1))
    

df_concat = df[['StreamingId','UserID','StreamingDateTime','StreamDuration']]
df_concat['sum_StreamDuration'] = 0

user_id_concat_list = df_concat['UserID'].tolist()
user_id_sum_list = result_df['UserID'].tolist()
user_id_not_duplicate = list(set(user_id_concat_list)-set(user_id_sum_list))
# print(len(user_id_not_duplicate))

result_df_not_dup = pd.DataFrame()
for index, item in enumerate(user_id_not_duplicate):
    print(index)
    df_not_dup = df_concat[df_concat['UserID']==item]
    result_df_not_dup = pd.concat([result_df_not_dup,df_not_dup])

df_result = pd.concat([result_df,result_df_not_dup], ignore_index=True)
df_result = df_result.drop_duplicates(subset=['StreamingId','UserID'])
df_result = df_result.sort_values(by='StreamingDateTime')

print(start_time,datetime.today())
print(df_result)

order_list = []
winner_list = []

for index,row in df_result.iterrows():
    if row['sum_StreamDuration'] >= 3000:
        order_list.append(index)
    else:
        order_list.append(None)
number = 1
for index,data in enumerate(order_list):
    print(index)
    if data is not None:
        winner_list.append("{}{}".format('ลำดับที่ ',number))
        number+=1
    else:
        winner_list.append("ไม่ผ่านเกณฑ์")

# # print(winner_list)

df_result['winner'] = winner_list
df_result =df_result[['UserID','StreamingDateTime','StreamDuration','sum_StreamDuration','winner']]
print(df_result)


df_result.to_excel(r'result/result_winner_result.xlsx',index=False)
