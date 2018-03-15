# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns




matplotlib.rcParams.update({'font.size': 10})
auth_user_df=pd.read_csv('./data/auth_user.csv')
dict_account = auth_user_df.set_index('id')['username'].to_dict()

# retrieve the df for a certain user, on a certain day, on a certain device
def convert_to_epoch_end(timestamp):
    time_string = timestamp [:19]
    tm = dt.strptime(time_string, '%Y-%m-%d %H:%M:%S')
    tm = tm - timedelta(seconds=tm.second%15,microseconds=tm.microsecond)+timedelta(seconds=15)
    return tm

def parse_date(timestamp):
    date_string = timestamp[:10] 
    return (dt.strptime(date_string, '%Y-%m-%d'))

def parse_datetime(timestamp):
    time_string = timestamp [:19]
    tm = dt.strptime(time_string, '%Y-%m-%d %H:%M:%S')
    return tm

def office_hour(timestamp):
    timestamp.time()
    arrive=dt(2020,1,1,7,30).time()
    leave =dt(2020,1,1,18,30).time()
    if (timestamp.time() <leave) and (timestamp.time()>=arrive):
        return True
    else:
        return False
    
def drop_non_office_hour(df,column_time):
    df['office_hour']=df[column_time].apply(office_hour)
    df= df[df['office_hour']==True].drop(['office_hour'],axis=1) 
    return df
    
def correct_id(user_id):
    if user_id == 34:
        return 35
    elif user_id == 36:
        return 38
    elif user_id == 33:
        return 41
    else:
        return user_id
    
def clean_data(CPE_df):
    
    CPE_df['user_id'] = CPE_df['user_id'].apply(correct_id)
    CPE_df['filter9']=(CPE_df['user_id']==50) & (CPE_df['date']=='2018-02-27')&(CPE_df['current_epoch_end']< '2018-02-27 10:15:00')
    CPE_df['filter8']=(CPE_df['user_id']==55) & (((CPE_df['date']=='2018-02-16')&(CPE_df['current_epoch_end']< '2018-02-16 10:25:00'))|(CPE_df['date']=='2018-02-14'))
    CPE_df['filter1']=(CPE_df['user_id']==31 )&((CPE_df['date']=='2017-11-06')|(CPE_df['date']=='2017-11-14')|((CPE_df['current_epoch_end']>'2017-11-10 13:33')&(CPE_df['date']=='2017-11-10'))|(CPE_df['date']=='2017-11-22'))
    CPE_df['filter2']=(CPE_df['user_id']==35) & (CPE_df['date']=='2017-11-01') &(CPE_df['current_epoch_end']< '2017-11-01 11:47:00')
    CPE_df['filter3']=(CPE_df['user_id']==30 )&((CPE_df['date']=='2017-11-06')|(CPE_df['date']=='2017-12-01')|(CPE_df['date']=='2017-12-10'))
    CPE_df['filter4']= ((CPE_df['user_id']==37)|(CPE_df['user_id']==30)|(CPE_df['user_id']==32)|(CPE_df['user_id']==29))&((CPE_df['date']>='2017-11-10')&(CPE_df['date']<='2017-11-15'))
    CPE_df['filter5']=(CPE_df['user_id']==41)&((CPE_df['date']=='2017-11-11')|(CPE_df['date']=='2017-11-24'))
    CPE_df['filter6']=(CPE_df['user_id']==42)&((CPE_df['date']=='2017-11-22')|(CPE_df['date']=='2017-11-11')|((CPE_df['date']=='2017-11-29')&(CPE_df['current_epoch_end']<'2017-11-29 06:00:00')))
    CPE_df['filter7']=(CPE_df['user_id']==39)&((CPE_df['date']=='2017-11-23')|(('2017-11-29'<CPE_df['date'])&(CPE_df['date'] < '2017-12-07')))
    CPE_df = CPE_df[(CPE_df ["filter2"]==False)&(CPE_df['filter1']== False)&(CPE_df['filter3']== False)&(CPE_df['filter4']== False)&(CPE_df['filter5']==False)&(CPE_df['filter6']==False)&(CPE_df['filter7']==False)&(CPE_df['filter8']==False)&(CPE_df['filter9']==False)].drop(['filter2','filter1', 'filter3','filter4','filter5','filter6','filter7','filter8','filter9'],axis = 1)   
    CPE_df=CPE_df.reset_index(drop=True)
    return CPE_df


    
def retrieve(original_df, user_id,device,date):
    df = original_df[(original_df['user_id']==user_id) & (original_df['deviceType']== device) &(original_df['date']== date)].drop(['date'],axis =1)
    df['difference']= df ['current_epoch_end'].diff()
    df.set_index('current_epoch_end', drop=True,inplace=True)   
    if len(df.index.values)>0:
        df.loc[df.index.values[0],'difference']=timedelta(seconds = 15) 
    df['valid_wear']= (df['difference']<timedelta(minutes=10))&(df['difference']>=timedelta(0))
    print ('dataframe head for user{} on {}'.format(user_id,date),':\n',df.head(2))   
    print ('dataframe tail for user{} on {}'.format(user_id,date),':\n',df.tail(2))
    #print ('start tracking time:\n',tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)&(tracking_status['status']=='t')].head(1))
    #print ('end tracking time:\n',tracking_status[(tracking_status['user_id']==user_id)&(tracking_status['date']==date)&(tracking_status['status']=='f')].tail(1))
    return df


# quick n dirty calculation the valid wear durations and non-valid durations for /day/participant 
def get_wear_time(PxDx):
    if type(PxDx) == pd.DataFrame:
        grouped_by_valid_wear= PxDx.groupby('valid_wear')
        try:
            print ('Invalid periods: \n',(grouped_by_valid_wear).get_group(False))
        except KeyError:
            pass 
        return (grouped_by_valid_wear['difference'].sum())

        
def fill_gaps (PxDx): 
    if type(PxDx) == pd.DataFrame:
        ts = pd.date_range(PxDx.index.values[0],PxDx.index.values[-1], freq='15S')
        ts = pd.Series([0], index=ts)
        base_df = pd.DataFrame({'CPE':ts})
        df2 = PxDx.drop(['user_id','deviceType','lite','valid_wear','difference'],axis = 1)
        return base_df.add(df2,fill_value=0)    
        
####################### activity detector ######################
#both take a list and threshold, return a bool
def break_end(x,C1): 
    for i in x:
        if i > C1:
            return False    
    return True 

def break_start(x,P):
    if x.sum()>= P:
        return True
    else:
        return False   
    

def classify_a_day(PxDx): #to-do: change duration calculation and plot to using transition to  
    if type(PxDx) == pd.DataFrame:
        C1 = 5
        x = 6
        C0 = 25
        P = 36
        Q= 2
        
        df_episodes= pd.DataFrame()
        #df_episodes.set_value(0,'episode_start', "2017-10-24 14:55:15")
        #df_episodes.set_value(0,'episode_end',"2017-10-24 14:55:15")
        CPE_df = PxDx
        CPE_df = CPE_df.reset_index(drop=False)
        CPE_df.rename(columns={'index': 'current_epoch_end'},inplace = True)
        CPE_df['label']=np.nan
        CPE_df['note']=''
        CPE_df['transition_from']=np.nan
        CPE_df['transition_from'].iloc[0] = 0
        
        CPE_df['zero']=int()
        continuous_zero_counter = 0 # for invalid periods  
        #current_episode_duration = 6*15 
        for index, row in CPE_df.iterrows():
            if row['CPE'] == 0:
                continuous_zero_counter = continuous_zero_counter +1
            else:
                continuous_zero_counter = 0
            CPE_df.set_value (index, 'zero', continuous_zero_counter)

            # for the first 5
            if index < (x-1):
                CPE_df.set_value(index, 'label', 1)
                CPE_df.set_value(index,'note','start tracking')
            # invalid wear period if 0 counts over 10 minutes
            elif continuous_zero_counter >=40: 
                for i in range(40):
                    CPE_df.set_value(index-i, 'label', -1) #set [index-39:index+1] row as invalid  
                if (CPE_df.iloc[(index - continuous_zero_counter)]['label']== 0):
                    CPE_df.set_value(index - continuous_zero_counter+1, 'transition_from', 0)
                elif(CPE_df.iloc[(index - continuous_zero_counter)]['label']== 1):
                    CPE_df.set_value(index - continuous_zero_counter+1, 'transition_from', 1)
            # valid wear
            else:
                # if last episode was inactive, use break detector mode  
                if ((CPE_df.iloc[(index-1)]['label']== 0)): 
                    # and walking is detected
                    if(row['CPE']> C0): 
                        CPE_df.set_value(index, 'label', 1 )
                        CPE_df.set_value(index, 'transition_from', 0)
                        CPE_df.set_value(index,'note','walk')
                            
                    # and constant mild movements are detected
                    elif (break_start(CPE_df.CPE.iloc[(index-Q+1):(index+1)],P)): 
                        CPE_df.set_value(index-Q+1, 'transition_from', 0)
                        for i in range (Q):
                            CPE_df.set_value(index-i, 'label', 1) #set [index-3:index+1] row as active
                            CPE_df.set_value(index-i,'note','constant and mild detected')                   
                    else:
                        CPE_df.set_value(index, 'label', 0 )
                # last epoch was invalid, also use break detector mode 
                elif ( CPE_df.iloc[(index-1)]['label']== -1): 
                    # and walking is detected
                    if(row['CPE']> C0): 
                        CPE_df.set_value(index, 'label', 1 )
                        CPE_df.set_value(index, 'transition_from',-1)

                    # and constant mild movements are detected
                    elif (break_start(CPE_df.CPE.iloc[(index-Q+1):(index+1)],P)): 
                        CPE_df.set_value(index-Q+1, 'transition_from',-1)
                        for i in range (Q):
                            CPE_df.set_value(index-i, 'label', 1) #set [index-3:index+1] row as active
                            CPE_df.set_value(index-i,'note','constant and mild detected')
                        
                    else:
                        CPE_df.set_value(index, 'label', 0 )
                        CPE_df.set_value(index, 'transition_from',-1)


                # last eposch was acitve, use break register mode
                else : 
                    # and break_end is detected
                    if break_end(CPE_df.CPE.iloc[(index-x+1):(index+1)],C1): 
                        CPE_df.set_value(index-x+1, 'transition_from', 1)
                        for i in range(x):
                            CPE_df.set_value(index-i, 'label', 0) #set [index-x+1:index+1] row as inactive 
                            CPE_df.set_value(index-i,'note','break end detected')
                        
                    else:
                        CPE_df.set_value(index,'label',1)
                        
        CPE_df['transition_from'].iloc[-1] = CPE_df['label'].iloc[-2]

        df_episodes = CPE_df[np.isnan(CPE_df['transition_from'])==False]               
        return (CPE_df,df_episodes)

def classify_cup_movement(PxDx): #to-do: change duration calculation and plot to using transition to  
    if type(PxDx) == pd.DataFrame:
        C1 = 0 # end of walk
        x = 20 # end of drink 
        C0 = 0 # drink detection threshold 
        P = 10 # walk detection threshold 
        Q= 2
        
        df_episodes= pd.DataFrame()
        #df_episodes.set_value(0,'episode_start', "2017-10-24 14:55:15")
        #df_episodes.set_value(0,'episode_end',"2017-10-24 14:55:15")
        CPE_df = PxDx
        CPE_df = CPE_df.reset_index(drop=False)
        CPE_df.rename(columns={'index': 'current_epoch_end'},inplace = True)
        CPE_df['label']=np.nan
        CPE_df['note']=''
        CPE_df['transition_from']=np.nan
        
        #current_episode_duration = 6*15 
        for index, row in CPE_df.iterrows():
            # for the first 5 minutes, assume setting up, no drink 
            if index < (x-1):
                CPE_df.set_value(index, 'label', 0)
                CPE_df.set_value(index,'note','start day')
            else:
                # if last episode was still, use walk/drink detector mode  
                if ((CPE_df.iloc[(index-1)]['label']== 0)): 
                    # and walking is detected
                    if (break_start(CPE_df.CPE.iloc[(index-Q+1):(index+1)],P)): 
                        CPE_df.set_value(index-Q+1, 'transition_from', 0)
                        for i in range (Q):
                            CPE_df.set_value(index-i, 'label', 2) #set [index-3:index+1] row as active
                            CPE_df.set_value(index-i,'note','walk with cup')
                    elif(row['CPE']> C0): 
                        CPE_df.set_value(index, 'transition_from', 0)
                        CPE_df.set_value(index, 'label', 1 )
                        CPE_df.set_value(index,'note','drink')                                    
                    else:
                        CPE_df.set_value(index, 'label', 0 )
                # if last episode was was walking
                elif  ((CPE_df.iloc[(index-1)]['label']== 2)): 
                    # and end of tea break is detected
                    if break_end(CPE_df.CPE.iloc[(index-x+1):(index+1)],C1): 
                        CPE_df.set_value(index-x+1, 'transition_from', 2)
                        for i in range(x):
                            CPE_df.set_value(index-i, 'label', 0) #set [index-x+1:index+1] row as inactive 
                            CPE_df.set_value(index-i,'note','end of tea/water break') 
                    else:
                        CPE_df.set_value(index,'label',2)
                # if last episode was drink
                else: 
                    if (break_start(CPE_df.CPE.iloc[(index-Q+1):(index+1)],P)): 
                        CPE_df.set_value(index-Q+1, 'transition_from', 1)
                        for i in range (Q):
                            CPE_df.set_value(index-i, 'label', 2) #set [index-3:index+1] row as active
                            CPE_df.set_value(index-i,'note','walk with cup')
                    elif break_end(CPE_df.CPE.iloc[(index-x+1):(index+1)],C1): 
                        CPE_df.set_value(index-x+1, 'transition_from', 1)
                        for i in range(x):
                            CPE_df.set_value(index-i, 'label', 0) #set [index-x+1:index+1] row as inactive 
                            CPE_df.set_value(index-i,'note','end of drinking event') 
                    else:
                        CPE_df.set_value(index, 'label', 1)
                        
        CPE_df['transition_from'].iloc[-1] = CPE_df['label'].iloc[-2]

        df_episodes = CPE_df[np.isnan(CPE_df['transition_from'])==False]               
        return (CPE_df,df_episodes)
#####################################################

def get_day_stats(episodes_df):
    if type(episodes_df) == pd.DataFrame: 
        df = episodes_df
        df['prolonged']=bool()
        df['prolonged']=(df['transition_from']==0) & (df['last_episode_duration']>timedelta(minutes=60))
        df['sustained']=(df['transition_from']==0) & (df['last_episode_duration']>timedelta(minutes=30))
        dict1= {'daily inactive': df[df['transition_from']==0]['last_episode_duration'].sum(),
                'daily active':df[df['transition_from']==1]['last_episode_duration'].sum(),
                'daily invalid': df[df['transition_from']==-1]['last_episode_duration'].sum(),
                'daily valid':df[(df['transition_from']==1)|(df['transition_from']== 0)]['last_episode_duration'].sum(),
                'longest sitting': df[df['transition_from']==0]['last_episode_duration'].max(),
                 'prolonged sitting events':df['prolonged'].sum(),
                'total prolonged sitting': df[df['prolonged']]['last_episode_duration'].sum(),
                'total sustained sitting': df[df['sustained']]['last_episode_duration'].sum(),
                'median sitting duration': df[df['transition_from']==0]['last_episode_duration'].median()}
        print ('\n Lists of Prolonged Sitting Episodes: \n',df[(df['transition_from']==0) & (df['last_episode_duration']>timedelta(minutes=60))].drop(['CPE',
        'transition_to','zero','transition_from'],axis=1))
        return dict1
    
def get_day_drink_stats(df):
    if type(df) == pd.DataFrame: 
        grouped = df.groupby('transition_from')['last_episode_duration']
        dict = {'drink_event_count': df[(df['transition_from']==1)]['last_episode_duration'].count(),
            'water_break_count': df[(df['transition_from']==2)]['last_episode_duration'].count()   , 
            'total_water_break_duration': df[(df['transition_from']==2)]['last_episode_duration'].sum(),
            'median_drink_frequency':df[(df['transition_from']==0)]['last_episode_duration'].median() 
              }
        return dict
    
def process_all(PxDx):
    if type(PxDx)==pd.DataFrame:
        PxDx_df_filled = fill_gaps(PxDx) # return a filled df
        output = classify_a_day(PxDx_df_filled)
        df_episodes =output[1].rename(columns ={'current_epoch_end':'current_episode_end','label':'transition_to'})
        df_episodes = df_episodes.reset_index(drop = True,inplace=False)
        df_episodes['last_episode_duration'] = df_episodes['current_episode_end']-df_episodes['current_episode_end'].shift(1)
        day_stats = get_day_stats(df_episodes)
        return day_stats

############### Plot the day #####################
def rescale(x):
    if x ==1:
        return 1
    elif x == 0:
        return 0
    else:
        return -0.09

######################################################## 

def plot_a_day(PxDx_labeled,df_episodes,second_df, PxDx_tracking_status, PxDx_connection_status, PxDx_reminder, username,date):
    PxDx_labeled = PxDx_labeled.drop(['zero','transition_from','note'],axis=1).set_index('current_epoch_end',drop=True)
    PxDx_labeled ['activity status'] =  PxDx_labeled['activity status'].apply(rescale)
    ax = PxDx_labeled['activity status'].plot(color = 'black', label = 'activity classification')
    ax.set_title(username + ' ' + date)
    ax.set_ylim(bottom = -0.1, top = 2)
    #ax.set_xlim(PxDx_tracking_status.index.values[0],PxDx_tracking_status.index.values[-1])
    ax.set_yticks([-0.1,0,1,1.3,1.4,1.45,1.5])
    #ax.set_xlim(pd.to_datetime('08:00'), pd.to_datetime('18:00'))
    ax.tick_params(labelsize=10)
    
    
    ax.set_yticklabels(['invalid','inactive','active','reminder','cup_connection','wrist_connection','tracking_status'])
    
    
    ax2= PxDx_labeled['CPE'].plot(style = ':', color ='grey', label = 'wrist movement intensity',secondary_y=True, ax=ax)
    ax2.set_ylim(-4,70)
    try: 
        second_df['CPE'].plot(style= '.', color = '#00BFFF', label = 'cup movement intensity', secondary_y = True,ax=ax)    
    except TypeError:
        print ('no cup movement data')
    
    try:
        PxDx_tracking_status['start_tracking'].plot(style = '>', color = 'g', ax=ax)
        PxDx_tracking_status['stop_tracking'].plot(style = '+', color = 'r', ax=ax)
    except TypeError:
        pass
    
    try:
        PxDx_connection_status['wrist_connected'].plot(style = "2", color ='b', label='connected', ax=ax)
        PxDx_connection_status['wrist_disconnected'].plot(style = '|', color = 'r', label = 'disconnected', ax=ax)
        PxDx_connection_status['cup_connected'].plot(style = '2', color='#00BFFF', label = '', ax=ax)
        PxDx_connection_status['cup_disconnected'].plot(style = '|',color = 'r', label = '', ax=ax)
    except TypeError:
        pass
    
    try:
        PxDx_reminder[PxDx_reminder['reminder']==1.3]['reminder'].plot(style = '*', color = '#FF9900', ax=ax)
        #PxDx_reminder['snooze'].plot(style = '|', color = '#AAAAAA', ax=ax)
    except:
        pass
    
    
    ax.legend(loc=2)
    plt.legend(loc=1)
    # add text displaying duration from df_episodes
    for i in range(1,len(df_episodes)):
        position_y = rescale(df_episodes.transition_from[i])-0.02
        if df_episodes.last_episode_duration[i].seconds < 60:
            duration_to_show = ''
            position_x = 0
        else:
            h = df_episodes.last_episode_duration[i].seconds//3600
            m = (df_episodes.last_episode_duration[i].seconds - h*3600)//60
            s =  df_episodes.last_episode_duration[i].seconds- h*3600 - m*60
            if h  == 0:
                duration_to_show = str(m) + 'min'
            else:
                duration_to_show = str(h) + 'h' + str(m)+ 'min'
            position_x = df_episodes.current_episode_end[i] - timedelta (seconds = df_episodes.last_episode_duration[i].seconds)

        if df_episodes.transition_from[i]==1:
            colour = '#2C6700' #green
            position_y = position_y + 0.1
        elif df_episodes.transition_from[i]== -1:
            colour = '#AAAAAA' #grey
            position_y = position_y + 0.1
        elif df_episodes.last_episode_duration[i].seconds >= 3600:
            colour = 'red'
        else:
            colour = '#FF9900'#amber      
        ax.text(position_x, position_y, s=duration_to_show, fontsize=10,rotation=30, color= colour)
    
    for index, row in  PxDx_reminder.iterrows():
        if (row['reminder']== 1.3)| (row['reminder']== 1.1):
            ax.text(index,row['reminder']-0.05,s=row['action'],fontsize=8, rotation=-30)
    print (ax.get_xlim())
    return ax

def convert_reminder_type(action_string):
    if 'break' in action_string:
        return 1.3
    elif action_string[:5]=='pause':
        return 1.2
    elif action_string[:7]=='unpause':
        return 1.4
    elif action_string[:4]=='none':
        return 1.1 