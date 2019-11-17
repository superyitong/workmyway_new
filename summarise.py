# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from workmyway_methods import (fill_gaps, office_hour, drop_non_office_hour, classify_a_day, classify_cup_movement, 
                                 convert_to_epoch_end,get_day_stats,get_day_drink_stats, retrieve, correct_id, plot_a_day, parse_date,
                                 parse_datetime,clean_data, convert_reminder_type,convert_to_minutes)

#####  read data and conver data types ########
raw_count = pd.read_csv("./data/smartcup_server_stepreading.csv")

raw_count['date']=raw_count['timestamp'].apply(parse_date)
raw_count = raw_count[raw_count.date>datetime(2017,10,25)]
raw_count['current_epoch_end'] = raw_count['timestamp'].apply(convert_to_epoch_end)
raw_count.drop(['appID','timestamp'], axis = 1, inplace = True) 
raw_count.loc[((raw_count['user_id']==54) & (raw_count['date']<'2018-03-15')),'user_id']=58
raw_count[raw_count['user_id']==58][['current_epoch_end','lite']]


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
reminder = reminder[(reminder.date>datetime(2017,10,25))&(reminder.lite=='f')]
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
dict_id = {30:'1',31:'2',32:'3',41:'4',37:'5', 35:'6', 38:'7', 39:'8', 42:'9', 50:'10',51:'11',52:'12',58:'13',53:'14',55:'15'}


CPE_df['office_hour']=CPE_df['current_epoch_end'].apply(office_hour)
CPE_df= drop_non_office_hour(CPE_df,'current_epoch_end')
connection_status= drop_non_office_hour(connection_status,'timestamp')
reminder= drop_non_office_hour(reminder,'timestamp')

# replace this user54's phone with 58 and use phone 54 for debugging after 3-14

connection_status.loc[((connection_status['user_id']==54) & (connection_status['date']<'2018-03-15')),'user_id']=58
tracking_status.loc[((tracking_status['user_id']==54) & (tracking_status['date']<'2018-03-15')),'user_id']=58
reminder.loc[((reminder['user_id']==54) & (reminder['date']<'2018-03-15')),'user_id']=58




#%%
goal_setting_record = pd.read_csv('./data/smartcup_server_configurationchange.csv') 
goal_setting_record=goal_setting_record[goal_setting_record['user_id'].isin(dict_id)]
goal_setting_record['date']=goal_setting_record['timestamp'].apply(parse_date)
goal_setting_record.loc[((goal_setting_record['user_id']==54) & (goal_setting_record['date']<'2018-03-15')),'user_id']=58

alert_setting_record=goal_setting_record[goal_setting_record['key'].str.contains("alert")]
days_alert_changed = alert_setting_record.drop_duplicates(['user_id','date'])
engagement_with_action_planning = days_alert_changed.groupby('user_id',as_index=False).date.count()
engagement_with_action_planning.rename(columns={'date':'engagement_with_action_planning(number of days)'},inplace=True)


goal_setting_record = goal_setting_record[goal_setting_record['key'].str.contains("goal")]
days_goal_changed = goal_setting_record.drop_duplicates(['user_id','date'])
engagement_with_goal_setting = days_goal_changed.groupby('user_id',as_index=False).date.count()
engagement_with_goal_setting.rename(columns={'date':'engagement_with_goal_setting(number of days)'},inplace=True)

engagement_with_action_planning['pID'] = engagement_with_action_planning['user_id'].map(dict_id)
engagement_with_goal_setting['pID'] = engagement_with_goal_setting['user_id'].map(dict_id)

engagement_with_action_planning.to_csv("../output/engagement_with_action_planning.csv",sep=',',index=False)
engagement_with_goal_setting.to_csv("../output/engagement_with_goal_setting.csv",sep=',',index=False)

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

#%%    
tracking_summary_by_day = pd.DataFrame()
full_alert_df =pd.DataFrame() 

for user_id in [30,31,32,41,37,35,38,39,42,50,51,52,58,53,55]:
    unique_date_list1 = CPE_df[(CPE_df['user_id']==user_id)& (CPE_df['deviceType']==0)].date.unique()
    unique_date_list2 = tracking_status[(tracking_status['user_id']==user_id)].date.unique()
    unique_date_set = pd.to_datetime((np.union1d(unique_date_list1,unique_date_list2)))
    start_date = unique_date_set[0] 
    intervention_start_date = CPE_df[(CPE_df['user_id']==user_id)& (CPE_df['deviceType']==0)&(CPE_df['lite']=='f')].date.iloc[0]
    for date in unique_date_set:
        #remove known out-of-office days; 
        if (user_id == 42) or (user_id == 52) or (user_id == 50):
            hld = pd.date_range('2017-12-25','2018-01-08').tolist()+pd.date_range('2018-03-30','2018-4-03').tolist()
            bdd = np.busdaycalendar(weekmask = '1111000', holidays=hld)
        elif user_id == 39:
            hld = pd.date_range('2017-12-25','2018-01-07').tolist()+pd.date_range('2017-12-03','2017-12-08').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        elif user_id == 32:
            hld = pd.date_range('2017-12-08','2018-01-07').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        elif user_id == 58:
            hld = pd.date_range('2018-2-19','2018-2-20').tolist()+pd.date_range('2018-03-14','2018-3-15').tolist()+pd.date_range('2018-03-30','2018-4-03').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        elif user_id == 55:
            hld = pd.date_range('2018-03-30','2018-4-03').tolist()+pd.date_range('2018-04-09','2018-04-13').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        else:
            hld = pd.date_range('2017-12-25','2018-01-02').tolist()+pd.date_range('2018-03-30','2018-4-03').tolist()
            bdd = np.busdaycalendar(holidays=hld)
        day_num = np.busday_count(start_date,date, busdaycal = bdd)
        day_num_since_intervention = np.busday_count(intervention_start_date,date, busdaycal = bdd)
        
        try: 
            PxDx_tracking_status = tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)]
            turn_on = PxDx_tracking_status[(tracking_status['status']=='t')]['timestamp'].iloc[0].time()
        except IndexError:
            turn_on = np.nan
            
        try: 
            PxDx_tracking_status = tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)]
            turn_off = PxDx_tracking_status[(tracking_status['status']=='f')]['timestamp'].iloc[-1].time()
            #baseline= {'lite':PxDx_tracking_status.lite.iloc[0]}
        except IndexError:
            turn_off = np.nan
            
        PxDx_df = retrieve(CPE_df, user_id,0,date)
        PxDx_df_cup = retrieve(CPE_df, user_id,1,date)
        try:
            first_data_point = pd.to_datetime(PxDx_df.index.values)[0].time()
            last_data_point =pd.to_datetime(PxDx_df.index.values)[-1].time() 
        except IndexError:
            first_data_point = np.nan
            last_data_point = np.nan

        metadata_dict = {'date':date,'user_id':user_id, 'Turn_ON':turn_on,'Turn_OFF':turn_off,'first_reading':first_data_point,'last_reading':last_data_point,'day':day_num,'intervention_day':day_num_since_intervention,'start_date':start_date,
                         'intervention_start_date':intervention_start_date}   
        
        if len(PxDx_df.index.values)>4: # there is step reading
            baseline= {'lite':PxDx_df.lite[0]}
            PxDx_df_filled = fill_gaps(PxDx_df) # return a filled df
            output = classify_a_day(PxDx_df_filled)
            #PxDx_labeled = output [0]
            df_episodes =output[1].rename(columns ={'current_epoch_end':'current_episode_end','label':'transition_to'})
            df_episodes.reset_index(drop = True,inplace=True)
            df_episodes['transition_to'].iloc[-1]=1 # leave office 
            df_episodes['last_episode_duration'] = df_episodes['current_episode_end']-df_episodes['current_episode_end'].shift(1)
            #####return and print daily stats ######
            day_summary = get_day_stats(df_episodes)
            sum_dict = {**metadata_dict,**day_summary,**baseline}
            if len(PxDx_df_cup.index.values)>4: # there is cup reading
                output_cup = classify_cup_movement(fill_gaps(PxDx_df_cup))
                df_episodes_cup = output_cup[1].rename(columns = {'current_epoch_end':'current_episode_end','label':'transition_to'})
                df_episodes_cup.reset_index(drop = True,inplace=True)
                df_episodes_cup['last_episode_duration']=df_episodes_cup['current_episode_end'].diff(1)
                day_drink_summary= get_day_drink_stats(df_episodes_cup)
                sum_dict= {**sum_dict,**day_drink_summary}
            PxDx_reaction_df= df_episodes[['current_episode_end','transition_to']]
            PxDx_reaction_df = df_episodes[df_episodes['transition_to']==1]
            #PxDx_invalid_transition = df_episodes[(df_episodes['transition_to']==-1)|(df_episodes['transition_from']==-1)]
       
            try:
                #TO-DO: remove invalid tracking period: add a 'label' column to PxDx_alert , take the value from the nearest row (and less than 5 min away) in PxDx_df_labeled
                #TO-DO: if 'response_latency' > 1 hours AND next transition_to is '-1
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
                        #invalid_point = PxDx_invalid_transition[PxDx_invalid_transition['current_episode_end']>reminder_time].reset_index()['current_episode_end'].iloc[0]
                    except IndexError:
                        raise                    
                    PxDx_alert.set_value(index,'reaction',reaction_time)
                    
                PxDx_alert['response_latency']=PxDx_alert['reaction']-PxDx_alert['timestamp']
                PxDx_alert['reaction_to_the_next_reminder']=PxDx_alert['reaction'].shift(-1)
                PxDx_alert['unique_reaction']=PxDx_alert['reaction_to_the_next_reminder']!=PxDx_alert['reaction']
                
                full_alert_df=pd.concat([full_alert_df,PxDx_alert])
                PxDx_alert.set_index('timestamp',inplace=True) 
                reminder_dict = {'reminders_triggered':len(PxDx_alert.index.values),'responses': PxDx_alert['response_latency'],'mean_latency':PxDx_alert['response_latency'].mean()}
                sum_dict = {**sum_dict,**reminder_dict}
            except:
                pass
        else:# there is no step reading (just tracking)
            sum_dict= metadata_dict
       
        
        tracking_summary_by_day= tracking_summary_by_day.append(sum_dict,ignore_index=True)

  
tracking_summary_by_day['lite'].fillna(method='ffill',inplace=True)

tracking_summary_by_day[['total prolonged sitting','total sustained sitting','daily invalid','daily valid','daily active','daily inactive','longest sitting','total_water_break_duration']]\
=tracking_summary_by_day[['total prolonged sitting','total sustained sitting','daily invalid','daily valid','daily active','daily inactive','longest sitting','total_water_break_duration']].fillna(value=timedelta(0))
tracking_summary_by_day=tracking_summary_by_day.applymap(convert_to_minutes)


tracking_summary_by_day[['reminders_triggered', 'prolonged sitting events','drink_event_count','water_break_count']]\
=tracking_summary_by_day[['reminders_triggered','prolonged sitting events','drink_event_count','water_break_count']].fillna(0)

tracking_summary_by_day = tracking_summary_by_day[tracking_summary_by_day['intervention_day']!=0] # remove the installation day
tracking_summary_by_day = tracking_summary_by_day[tracking_summary_by_day['day']!=0] 

tracking_summary_by_day.loc[(tracking_summary_by_day['intervention_day']<0),'period']='baseline'
tracking_summary_by_day.loc[(tracking_summary_by_day['intervention_day']>30),'period']='post-study'
tracking_summary_by_day.loc[((tracking_summary_by_day['intervention_day']>0) & (tracking_summary_by_day['intervention_day']<=30)),'period'] = 'intervention'


tracking_summary_by_day['day_validity'] = (tracking_summary_by_day['daily valid']>180)&(tracking_summary_by_day['daily invalid']<180)
tracking_summary_by_day['pID']=tracking_summary_by_day['user_id'].map(dict_id)


full_behavioural_data=tracking_summary_by_day[['pID','start_date','date','day','intervention_start_date','intervention_day','period','day_validity','Turn_ON','first_reading','Turn_OFF','last_reading','daily invalid',
                                                'daily valid','daily active','daily inactive',
                                                'total prolonged sitting','total sustained sitting',
                                                'prolonged sitting events','longest sitting','reminders_triggered','responses','mean_latency',
                                                'drink_event_count','water_break_count','total_water_break_duration','median_drink_frequency']]#

full_behavioural_data.to_csv("../output/behavioural.csv",sep=',',index=False)
#.applymap(lambda x: str(x)[7:] if type(x)==pd._libs.tslib.Timedelta else (x if type(x) == pd.Series else ('' if pd.isnull(x) else x)))\

process_measures= tracking_summary_by_day.groupby(["pID","period","day_validity"],as_index=False)['day'].count()
process_measures.to_csv("../output/process_measures.csv",sep=',',index=False)

valid_days = tracking_summary_by_day[(tracking_summary_by_day['day_validity'])]
valid_days.loc[((valid_days['intervention_day']>20)&(valid_days['intervention_day']<=30)),'period']='post-intervention'
pre_post = valid_days.groupby(['pID','period'],as_index=False)['daily active','daily inactive',
                                                'total prolonged sitting','total sustained sitting',
                                                'prolonged sitting events','longest sitting'].mean()
pre_post.to_csv("../output/pre-post.csv",sep=',',index=False)

#need to remove the invalid days from the full_alert_df before output

full_alert_df['pID']=full_alert_df['user_id'].map(dict_id)
full_alert_df.applymap(lambda x: str(x)[7:] if type(x)==pd._libs.tslib.Timedelta else ('' if pd.isnull(x) else x)).to_csv("../output/reminders.csv",sep=',',index=False)

#%%
full_alert_df['response_latency_minutes']=full_alert_df['response_latency'].map(lambda x: x.total_seconds()/60 )
prompts_received_by_day = full_alert_df.groupby(['pID','date'],as_index=False)['response_latency_minutes'].count()
prompts_dosage = pd.DataFrame(prompts_received_by_day.groupby(['pID'])['response_latency_minutes'].max())
prompts_dosage.rename(columns={'response_latency_minutes':'max_received_daily'},inplace=True)
prompts_dosage['total_received']= prompts_received_by_day.groupby('pID')['response_latency_minutes'].sum()
prompts_dosage['mean_latency']= full_alert_df.groupby(['pID'])['response_latency_minutes'].mean()
prompts_dosage['max_latency']= full_alert_df.groupby(['pID'])['response_latency_minutes'].max()
prompts_dosage['std_latency']= full_alert_df.groupby(['pID'])['response_latency_minutes'].std()
prompts_dosage['min_latency']=full_alert_df.groupby(['pID'])['response_latency_minutes'].min()

prompts_dosage.to_csv("../output/prompts_dosage.csv",sep=',')
#.to_csv("../output/prompts_received_by_day.csv",sep=',',index=False)
