import pandas as pd

def extract(covid_url,john_hopkins_data):

    print ("************************************************************************************************")
    url = covid_url
    if url =='':
        url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    df = pd.read_csv(url,error_bad_lines=False)

    ########################################
    # TRANSFORMATION STARTS HERE
    ########################################
    # changing the type of the column 'date' as DATE type
    df2 = df.astype({'date':'datetime64[ns]','cases':'int64','deaths':'int64'})
    #print(type(df2))
    #Retriving recent rows by filtering rows from last run date
    #greater than last run date
    #Depending on the type of the load ,rows should be filtered
    last_run_date = '2020-09-18'
    #df3 = df2[(df['date'] > last_run_date)]
    #print('*******************************************')

    #get John Hopins Data and combine it with current dates
    #print ("************************************************************************************************")
    john_hopkins_dataset_url = john_hopkins_data
    if john_hopkins_dataset_url == '':
        john_hopkins_dataset_url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1597116776410r0.9370121973993177"

    john_hopkins_data = pd.read_csv(john_hopkins_dataset_url,error_bad_lines=False)
    country_list = ['US','us']
    john_hopkins_data = john_hopkins_data[john_hopkins_data['Country/Region'].isin(country_list) ]
    #print(john_hopkins_data)
    #selecting Date and recovered column
    john_hopkins_data_recovered = john_hopkins_data[['Date','Recovered']]

    #print(john_hopkins_data_recovered)
    john_hopkins_data_recovered = john_hopkins_data_recovered.astype({'Date':'datetime64[ns]','Recovered':'int64'})
    #print(john_hopkins_data_recovered)

    #print('******************************************')
    #Merging two datasets based on date
    final_df = df2.join(john_hopkins_data_recovered.set_index('Date'), on ='date',how='left').fillna(0)
    #filtering records based on last run date
    #final_df = final_df[final_df['date'] >last_run_date]
    #print(final_df)
    return final_df
