     # -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from workmyway_methods import (fill_gaps, office_hour, drop_non_office_hour, classify_a_day, classify_cup_movement, 
                                 convert_to_epoch_end,get_day_stats,get_day_drink_stats, retrieve, correct_id, plot_a_day, parse_date,
                                 parse_datetime,clean_data, convert_reminder_type)

#####  read data and conver data types ########
raw_count = pd.read_csv("./data/smartcup_server_stepreading2.csv")
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
user_id =54
date = '2018-3-12'
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
#%%
'''
output_cup = classify_cup_movement(fill_gaps(PxDx_df_cup))
PxDx_labeled_cup = output_cup[0].rename(columns = {'label':'activity status'})
df_episodes_cup = output_cup[1].rename(columns = {'current_epoch_end':'current_episode_end','label':'transition_to'})
df_episodes_cup.reset_index(drop = True,inplace=True)
df_episodes_cup['last_episode_duration']=df_episodes_cup['current_episode_end'].diff(1)
get_day_drink_stats(df_episodes_cup)
'''
#df[(df['transition_from']==2)]['last_episode_duration'].median()
#mean_water_break_duration = df_episodes_cup[df_episodes_cup['transition_from']==]
#df_episodes_cup.groupby('transition_from').count()
#df_episodes_cup['last_episode_duration'].sum()
#%%    
tracking_summary_by_day = pd.DataFrame()
full_alert_df =pd.DataFrame() 

for user_id in [30,39,41,50,51,52,53,58,55]:#30,31,32,35,37,38,39,41,42
    unique_date_list1 = CPE_df[(CPE_df['user_id']==user_id)& (CPE_df['deviceType']==0)].date.unique()
    unique_date_list2 = tracking_status[(tracking_status['user_id']==user_id)].date.unique()
    unique_date_set = pd.to_datetime((np.union1d(unique_date_list1,unique_date_list2)))
    start_date = unique_date_set[0] 
    for date in unique_date_set:
        #remove out-of-office days; 
        if (user_id == 42) or (user_id == 52):
            hld = pd.to_datetime(pd.date_range('2017-12-25','2018-01-08')).tolist()
            bdd = np.busdaycalendar(weekmask = '1111000', holidays=hld)
        elif user_id == 39:
            hld = pd.date_range('2017-12-25','2018-01-07').tolist()+pd.date_range('2017-12-03','2017-12-08').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        elif user_id == 32:
            hld = pd.date_range('2017-12-08','2018-01-07').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        else:
            hld = pd.to_datetime(pd.date_range('2017-12-25','2018-01-02')).tolist()
            bdd = np.busdaycalendar(holidays=hld)
        day_num = np.busday_count(start_date,date, busdaycal = bdd)
        
        try: 
            PxDx_tracking_status = tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)]
            turn_on = PxDx_tracking_status[(tracking_status['status']=='t')]['timestamp'].iloc[0].time()
            turn_off = PxDx_tracking_status[(tracking_status['status']=='f')]['timestamp'].iloc[-1].time()
            #baseline= {'lite':PxDx_tracking_status.lite.iloc[0]}
        except IndexError:
            turn_on = np.nan
            turn_off = np.nan
            
        PxDx_df = retrieve(CPE_df, user_id,0,date)
        try:
            first_data_point = pd.to_datetime(PxDx_df.index.values)[0].time()
            last_data_point =pd.to_datetime(PxDx_df.index.values)[-1].time() 
        except IndexError:
            first_data_point = np.nan
            last_data_point = np.nan

        metadata_dict = {'date':date,'user_id':user_id, 'Turn_ON':turn_on,'Turn_OFF':turn_off,'first_reading':first_data_point,'last_reading':last_data_point,'day':day_num,'start_date':start_date}   
        
        if len(PxDx_df.index.values)>4: # there is step reading
            baseline= {'lite':PxDx_df.lite[0]}
            PxDx_df_filled = fill_gaps(PxDx_df) # return a filled df
            output = classify_a_day(PxDx_df_filled)
            df_episodes =output[1].rename(columns ={'current_epoch_end':'current_episode_end','label':'transition_to'})
            df_episodes.reset_index(drop = True,inplace=True)
            df_episodes['transition_to'].iloc[-1]=1 # leave office 
            df_episodes['last_episode_duration'] = df_episodes['current_episode_end']-df_episodes['current_episode_end'].shift(1)
            #####return and print daily stats ######
            day_summary = get_day_stats(df_episodes)
            sum_dict = {**metadata_dict,**day_summary,**baseline}
            if len(PxDx_df_cup.index.values)>1: # there is cup reading
                output_cup = classify_cup_movement(fill_gaps(PxDx_df_cup))
                df_episodes_cup = output_cup[1].rename(columns = {'current_epoch_end':'current_episode_end','label':'transition_to'})
                df_episodes_cup.reset_index(drop = True,inplace=True)
                df_episodes_cup['last_episode_duration']=df_episodes_cup['current_episode_end'].diff(1)
                day_drink_summary= get_day_drink_stats(df_episodes_cup)
                sum_dict= {**sum_dict,**day_drink_summary}
            PxDx_reaction_df= df_episodes[['current_episode_end','transition_to']]
            PxDx_reaction_df = df_episodes[df_episodes['transition_to']==1]  
            try:
                #TO-DO: remove invalid tracking period: add a 'label' column to PxDx_alert , take the value from the nearest row (and less than 5 min away) in PxDx_df_labeled
                #TO-DO: if 'reaction time' is the same as the previous row, remove 
                PxDx_alert = reminder[(reminder['user_id']==user_id)&(reminder['date']==date)]
                PxDx_alert ['reminder'] = PxDx_alert['action'].apply(convert_reminder_type)
                #PxDx_alert.reset_index(inplace=True,drop=True)
                PxDx_alert =PxDx_alert[PxDx_alert['reminder']==1.3]
                PxDx_alert['duration']=PxDx_alert['timestamp']-PxDx_alert['timestamp'].shift(1)
                PxDx_alert['unique_reminder']=PxDx_alert['duration']>timedelta(minutes=30)#considered same reminder if interval < 30 min 
                PxDx_alert['unique_reminder'].iloc[0]=True
                PxDx_alert=PxDx_alert[PxDx_alert['unique_reminder']==True]
                PxDx_alert= PxDx_alert[['timestamp','action','date','user_id']]
                for index, row in PxDx_alert.iterrows():
                    reminder_time = row['timestamp']
                    reaction_time=np.datetime64('NaT')
                    try:
                        reaction_time = PxDx_reaction_df[PxDx_reaction_df['current_episode_end']>reminder_time].reset_index()['current_episode_end'].iloc[0]
                    except IndexError:
                        raise
                    PxDx_alert.set_value(index,'reaction',reaction_time)
                PxDx_alert['response_latency']=PxDx_alert['reaction']-PxDx_alert['timestamp']
                PxDx_alert['reaction_to_the_next_reminder']=PxDx_alert['reaction'].shift(-1)
                PxDx_alert['unique_reaction']=PxDx_alert['reaction_to_the_next_reminder']!=PxDx_alert['reaction']
                
                full_alert_df=pd.concat([full_alert_df,PxDx_alert])
                PxDx_alert.set_index('timestamp',inplace=True) 
                reminder_dict = {'reminders_triggered':len(PxDx_alert.index.values),'response_latency': PxDx_alert['response_latency']}
                sum_dict = {**sum_dict,**reminder_dict}
            except:
                pass
        else:# there is no step reading (just tracking)
            sum_dict= metadata_dict
       
        
        tracking_summary_by_day= tracking_summary_by_day.append(sum_dict,ignore_index=True)

            
#tracking_summary_by_day['total prolonged sitting']=tracking_summary_by_day['total prolonged sitting'].apply(lambda x: 0 if pd.isnull(x) else x)
#tracking_summary_by_day['daily valid']=tracking_summary_by_day['daily valid'].apply(lambda x: 0 if pd.isnull(x) else x)
#tracking_summary_by_day['% prolonged sitting']=tracking_summary_by_day['total prolonged sitting']/tracking_summary_by_day['daily valid']
#tracking_summary_by_day['healthy sitting']=tracking_summary_by_day['daily inactive']-tracking_summary_by_day['total prolonged sitting']
tracking_summary_by_day['lite'] = tracking_summary_by_day['lite'].fillna(method='ffill')
#tracking_summary_by_day.sort_values('total prolonged sitting',inplace=True)
tracking_summary_by_day=tracking_summary_by_day[['user_id','start_date','date','day','lite','Turn_ON','first_reading','Turn_OFF','last_reading','daily invalid',
                                                'daily valid','daily active','daily inactive',
                                                'total prolonged sitting','total sustained sitting',
                                                'prolonged sitting events','longest sitting','reminders_triggered','response_latency']]#

tracking_summary_by_day.applymap(lambda x: str(x)[7:] if type(x)==pd._libs.tslib.Timedelta else (x if type(x) == pd.Series else ('' if pd.isnull(x) else x))).to_csv("../output/tracking_summary_by_day.csv",sep=',',index=False)
summary_of_valid_days = tracking_summary_by_day[(tracking_summary_by_day['daily valid']>timedelta(hours = 3))]
summary_of_valid_days.applymap(lambda x: str(x)[7:] if type(x)==pd._libs.tslib.Timedelta else x).to_csv("../output/valid_days_only.csv",sep=',',index=False)
full_alert_df.applymap(lambda x: str(x)[7:] if type(x)==pd._libs.tslib.Timedelta else ('' if pd.isnull(x) else x)).to_csv("../output/reminders.csv",sep=',',index=False)
#%%





