
from FinalExercise import torronto

#torronto = torronto()


class unitTesting:   
    
    #check the dataset  loaded properly or not
    def csv_load_test(path_1,path_2,path_3):
        df_refdata,df_trade,df_valuation = torronto.load_dataset_csv(path_1,path_2,path_3);
        if (df_refdata.empty and df_trade.empty and df_valuation.empty):
            print('DataFrame is empty!')
        else:
            print('Data Frames Loaded from CSV !')
        return(df_refdata,df_trade,df_valuation);
        
    #check the dataset  loaded properly or not
    def load_dataset_webservices_test(url):
        df_url= torronto.load_dataset_webservices(url);
        if (df_url.empty):
            print('DataFrame is empty!')
        else:
            print('Data Frames Loaded from Webservices!')
        return(df_url);
        
    #To test the join function, checked coloumn from three dataset its present or not into single dataframe
    def join_DataFrames_Test(df_refdata,df_trade,df_valuation):
        df_joined = torronto.joinDataFrames(df_refdata,df_trade,df_valuation)
    #checked coloumn from three dataset its present or not in df_joined
        if (('UQL_OC_MMB_MS' in df_joined.columns) and ('CLINE' in df_joined.columns) and ('MaturityDate' in df_joined.columns)):
            print('All 3 columns exits !')
        else:
            print('All columns do not exits !')
        return(df_joined);
        
   #check the breakstatus coloumn exist or not
    def breakstatus(df_joined):
        df_1 = torronto.breakstatus(df_joined)
        
        if ('breakstatus' in df_joined.columns):
            print('Breakstatus columns exits !')
        else:
            print('Breakstatus columns do not exits !')  
        return(df_1);
        
#check the Term coloumn created or not in dataframe
    def Term(df_1):
        df_2 = torronto.Term(df_1)
        
        if ('Term' in df_2.columns):
            print('Term columns exits !')
        else:
            print('Term columns do not exits !')  
        return(df_2);

    
    if __name__ == "__main__":
        
        path_1 = "D:/Interview/TD/Datasets/refdata.csv";
        path_2 = "D:/Interview/TD/Datasets/trade.csv";
        path_3 = "D:/Interview/TD/Datasets/valuation.csv";
        #Path for final dataset
        Final_path_file="D:/Interview/TD/Datasets/final_datset.csv";
        df_1, df_2, df_3 = csv_load_test(path_1,path_2,path_3)
        df_4 = join_DataFrames_Test(df_1, df_2, df_3)
        df_5 = breakstatus(df_4)
        df_6 = Term(df_5)
        
        #Store Final resultset dataset in Final_path_file path
        df_6.to_csv(Final_path_file,index=False);
        
        #To call test method for webservice
        url = 'https://api.covid19api.com/summary';
        df_url = load_dataset_webservices_test(url);
