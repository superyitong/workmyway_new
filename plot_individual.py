     # -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from workmyway_methods import (fill_gaps, office_hour, drop_non_office_hour, classify_a_day, classify_cup_movement, 
                                 convert_to_epoch_end,get_day_stats,get_day_drink_stats, retrieve, correct_id, plot_a_day, parse_date,
                                 parse_datetime,clean_data, convert_reminder_type)

#####  read data and conver data types ########
raw_count = pd.read_csv("./data/smartcup_server_stepreading.csv")
raw_count['date']=raw_count['timestamp'].apply(parse_date)
raw_count = raw_count[raw_count.date>datetime(2017,10,25)]
raw_count['current_epoch_end'] = raw_count['timestamp'].apply(convert_to_epoch_end)
raw_count.drop(['appID','timestamp'], axis = 1, inplace = True) 
# sum counts over epochs of 15 seconds 
CPE_df = raw_count.groupby(['user_id','deviceType','current_epoch_end','date','lite'],as_index=False).count().sort_values(
    ['current_epoch_end','user_id','deviceType','lite'])
CPE_df.rename(columns = {'id':'CPE'},inplace=True)
## delete/replace some datea with messed-up user_id 
CPE_df = clean_data(CPE_df)
# save CPE file for data backup 
CPE_df.to_csv('CPE.csv',sep=',',index=False)


connection_status = pd.read_csv('./data/smartcup_server_connectionstatus.csv')
connection_status['date']=connection_status['timestamp'].apply(parse_date)
connection_status = connection_status[connection_status.date>datetime(2017,10,25)]
connection_status['current_epoch_end'] = connection_status['timestamp'].apply(convert_to_epoch_end)
connection_status['timestamp']=connection_status['timestamp'].apply(parse_datetime)

connection_status.drop(['id','appID','expected'],axis =1,inplace=True) # timestamp, connected(t/f), deviceType, user_id, date
connection_status['user_id'] = connection_status['user_id'].apply(correct_id)

reminder= pd.read_csv('./data/smartcup_server_alert.csv')
reminder['date']=reminder['timestamp'].apply(parse_date)
reminder = reminder[reminder.date>datetime(2017,10,25)]
reminder['timestamp']=reminder['timestamp'].apply(parse_datetime)
reminder.drop(['id','appID'],axis =1,inplace=True) # timestamp,action (none(0,, user_id,date
reminder['user_id'] = reminder['user_id'].apply(correct_id)


tracking_status = pd.read_csv('./data/smartcup_server_trackingstatus.csv')
tracking_status['date']=tracking_status['timestamp'].apply(parse_date)
tracking_status =tracking_status[tracking_status.date>datetime(2017,10,25)]
tracking_status['timestamp']=tracking_status['timestamp'].apply(parse_datetime)
tracking_status.drop(['id','appID'],axis =1,inplace=True) # timestamp, status,user_id, date
tracking_status['user_id'] = tracking_status['user_id'].apply(correct_id)
tracking_status['current_epoch_end']=tracking_status['timestamp']
tracking_status['user_id'] = tracking_status['user_id'].apply(correct_id)
#tracking_status = clean_data(tracking_status)


auth_user_df=pd.read_csv('./data/auth_user.csv')
dict_account = auth_user_df.set_index('id')['username'].to_dict()

CPE_df['office_hour']=CPE_df['current_epoch_end'].apply(office_hour)
CPE_df= drop_non_office_hour(CPE_df,'current_epoch_end')
connection_status= drop_non_office_hour(connection_status,'timestamp')
reminder= drop_non_office_hour(reminder,'timestamp')

# replace this user54's phone with 58 and use phone 54 for debugging after 3-14
CPE_df.loc[((CPE_df['user_id']==54) & (CPE_df['date']<'2018-03-15')),'user_id']=58
connection_status.loc[((connection_status['user_id']==54) & (connection_status['date']<'2018-03-15')),'user_id']=58
tracking_status.loc[((tracking_status['user_id']==54) & (tracking_status['date']<'2018-03-15')),'user_id']=58
reminder.loc[((reminder['user_id']==54) & (reminder['date']<'2018-03-15')),'user_id']=58


#%%
user_id =30
date = '2018-02-16'
username = dict_account[user_id]
PxDx_tracking_status = tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)]
PxDx_tracking_status['start_tracking']= np.where(PxDx_tracking_status['status']=='t',1.5,np.nan)
PxDx_tracking_status['stop_tracking']= np.where(PxDx_tracking_status['status']=='f',1.5,np.nan)
PxDx_tracking_status = PxDx_tracking_status.set_index('timestamp',drop=True)#.drop(['status','user_id','lite','date'],axis =1)

PxDx_connection_status = connection_status[(connection_status['user_id']==user_id)&(connection_status['date']==date)]
PxDx_connection_status ['wrist_connected']= np.where((PxDx_connection_status['connected']=='t')&(PxDx_connection_status['deviceType']==0),1.45,np.nan)
PxDx_connection_status ['wrist_disconnected']= np.where((PxDx_connection_status['connected']=='f')&(PxDx_connection_status['deviceType']==0),1.45,np.nan)
PxDx_connection_status ['cup_connected']= np.where((PxDx_connection_status['connected']=='t')&(PxDx_connection_status['deviceType']==1),1.4,np.nan)
PxDx_connection_status ['cup_disconnected']= np.where((PxDx_connection_status['connected']=='f')&(PxDx_connection_status['deviceType']==1),1.4,np.nan)
PxDx_connection_status = PxDx_connection_status.set_index('timestamp',drop=True)


PxDx_reminder = reminder[(reminder['user_id']==user_id)&(reminder['date']==date)]
PxDx_reminder['reminder'] = PxDx_reminder['action'].apply(convert_reminder_type)
#PxDx_reminder['snooze']= PxDx_reminder['action'].apply(lambda x: 1.3 if (x[:5]=='pause') else np.nan)
PxDx_reminder = PxDx_reminder.set_index('timestamp',drop=True)

PxDx_df = retrieve(CPE_df, user_id,0,date)
PxDx_df_cup = retrieve(CPE_df, user_id,1,date)
try:    
    output = classify_a_day(fill_gaps(PxDx_df))
    df_episodes = output[1].rename(columns = {'current_epoch_end':'current_episode_end','label':'transition_to'})
    df_episodes = df_episodes.reset_index(drop = True,inplace=False)
    df_episodes['transition_to'].iloc[-1]=1 # leave office 
    df_episodes['last_episode_duration'] = df_episodes['current_episode_end']-df_episodes['current_episode_end'].shift(1)
    #####return and print daily stats ######
    print (get_day_stats(df_episodes))
    
    PxDx_labeled = output[0].rename(columns = {'label':'activity status'})
    #draw(PxDx_labeled,df_episodes,PxDx_tracking_status,username,date)
    plot_a_day(PxDx_labeled,df_episodes,PxDx_df_cup, PxDx_tracking_status,PxDx_connection_status,PxDx_reminder, username,date)
    plt.show()
except IndexError:
    print (PxDx_tracking_status)

try:
   output_cup = classify_cup_movement(fill_gaps(PxDx_df_cup))
   PxDx_labeled_cup = output_cup[0].rename(columns = {'label':'activity status'})
   df_episodes_cup = output_cup[1].rename(columns = {'current_epoch_end':'current_episode_end','label':'transition_to'})
   df_episodes_cup.reset_index(drop = True,inplace=True)
   df_episodes_cup['last_episode_duration']=df_episodes_cup['current_episode_end'].diff(1)
   print(get_day_drink_stats(df_episodes_cup))
except:
    pass

