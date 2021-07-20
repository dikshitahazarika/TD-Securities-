import pyodbc
import requests
import warnings
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
from datetime import date
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


#Unit Testing File is at bottom

class torronto:
    
###=======================================================================================================================
### FUNCTION DESCRIPTION : The function loads the dataset from CSV format files from local System and return three dataframe objects
###=======================================================================================================================


     def load_dataset_csv(path_1,path_2,path_3):
        df_refdata = pd.read_csv(path_1);
        df_trade = pd.read_csv(path_2);
        df_valuation = pd.read_csv(path_3);
        return (df_refdata,df_trade,df_valuation)

###=======================================================================================================================
###FUNCTION DESCRIPTION : The function loads the dataset from database using connection string,by providing appropiate values for server,database,username,password
### and the results of the sql queries store in three dataframe objects
###=======================================================================================================================


     def load_dataset_SQL_DataImport():
        server = 'servername' 
        database = 'TD' 
        username = 'Dikshita' 
        password = 'Password'  
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        query_1 = "SELECT * FROM valuation;"
        df_refdata = pd.read_sql(query_1, cursor)
        query_2 = "SELECT * FROM trade;"
        df_trade = pd.read_sql(query_2, cursor)
        query_3 = "SELECT * FROM refdata;"
        df_valuation = pd.read_sql(query_3, cursor)

        return (df_refdata,df_trade,df_valuation)


###=======================================================================================================================
###FUNCTION DESCRIPTION : The function loads the dataset from webservices and the results of the sql qiueries store in three dataframe objects
###=======================================================================================================================


     def load_dataset_webservices(df_url):
        r = requests.get(df_url)
        json = r.json()
        df_sample = pd.DataFrame([json['Global']])
        
        return (df_sample)

###=======================================================================================================================
###FUNCTION DESCRIPTION : This function use to  join the three dataset based on id's
###=======================================================================================================================

     def joinDataFrames(df_refdata, df_trade, df_valuation):
        df_1  = pd.merge( df_trade,df_refdata, on = 'Inventory', how='left',)
        df_2  = pd.merge( df_1 ,df_valuation, on = 'TradeId', how='left',)
        df_2['var_MS_PC'] = df_2['UQL_OC_MMB_MS'] - df_2['UQL_OC_MMB_MS_PC']
        df_2['var_MS_PC'].fillna(0,inplace=True)
        df_2['abs_var_MS_PC'] = df_2['var_MS_PC'].abs()
        
        return(df_2)
 
###=======================================================================================================================
### FUNCTION DESCRIPTION : This function use to  create a breakstatus coloumn and catogorized absolute difference in buckets in the dataset 
### =======================================================================================================================
      
     def breakstatus(df_2):  
        df_3 = df_2
        #calculating dataframe length for the loop
        count = len(df_3)
        df_3['breakstatus'] = ''
        for i in range(count):
            if(df_3['abs_var_MS_PC'][i]      <= 99.0) : 
                value = ('(0 - 99)')
                    
            elif(df_3['abs_var_MS_PC'][i]    <= 999.0)   : 
                value = ('(100 - 999)')
                    
            elif(df_3['abs_var_MS_PC'][i]    <= 9999.0)  : 
                value = ('(1000 - 9999)')
                    
            elif(df_3['abs_var_MS_PC'][i]    <= 99999.0) : 
                value = ('(10000 - 99999)')
                    
            else: 
                value = ('(100000 +)')
            df_3['breakstatus'][i] = value
        
        return (df_3)

###=======================================================================================================================
###FUNCTION DESCRIPTION : This function use to  create a Term coloumn and catogorized difference between  MaturityDate - Todaydate in buckets and also If MaturityDate is blank or already matured, set Term coloumn as blank
###=======================================================================================================================
    
    
     def Term(df_3):
        df_4 = df_3
        #Formatting MaturityDate as %Y%m%d 
        df_4['MaturityDate_format'] = pd.to_datetime(df_4['MaturityDate'], format= '%Y%m%d')
        # Difference between  MaturityDate - Todaydate
        df_4['Diff_date'] = (df_4['MaturityDate_format'] - pd.to_datetime(date.today()))
        # Convert datetime(delta) to string and  use slicing for taking no of days 
        df_4['Diff_date_2'] = ((df_4['Diff_date']).astype(str).str[0:-24])
        # Convert string  to int for calculation to catogorized in buckets
        df_4['Diff_date_3'] = ((df_4['Diff_date_2']).astype(str).astype(int))
        #calculating dataframe length for the loop
        count = len(df_4);
        df_4['Term'] = ''
        for i in range(count):
            
            if(df_4['Diff_date_3'][i]      <= 0.0) : 
                value = ('')
            elif(df_4['Diff_date_3'][i]    <= 30.0)   : 
                value = ('(0m - 1m)')             
            elif(df_4['Diff_date_3'][i]    <= 182.0)  : 
                value = ('(1m - 6m)')
            elif(df_4['Diff_date_3'][i]    <= 365.0) : 
                value = ('(6m - 1yr)')
            elif(df_4['Diff_date_3'][i]    <= 3650.0) : 
                value = ('(1yr - 10yr)')
            elif(df_4['Diff_date_3'][i]    <= 10590.0) : 
                value = ('(10yr - 30yr)')   
            elif(df_4['Diff_date_3'][i]    <= 18250.0) : 
                value = ('(30yr - 50yr)')   
            else: 
                value = ('(50yr +)')
            df_4['Term'][i] = value
    
        return (df_4)
   


"""
=======================================================================================================================
FUNCTION DESCRIPTION : Unit Test Cases for above method
=======================================================================================================================
"""






