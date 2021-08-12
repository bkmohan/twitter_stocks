import os, glob, datetime, requests, io, logging, time
import pandas as pd


# DATA_FOLDER = './data/stocks'

class StocksData():

    def __init__(self, data_folder):
        # logging.basicConfig(level=logging.DEBUG, filename='tweets.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')
        
        self.data_folder = data_folder
        all_files = glob.glob(os.path.join(data_folder, "*.csv"))
        self.stocks_df = {}

        logging.debug("Fetching Alphavantage APIs")
        self.apis =  alpha_apis()
        self.api = self.apis[0]
        
        logging.info(f"Reading stocks csv from {data_folder}")
        for file in all_files:
            file_name = os.path.splitext(os.path.basename(file))[0]
            dfn = pd.read_csv(file)
            dfn['time'] =  pd.to_datetime(dfn['time'])
            self.stocks_df[file_name] = dfn

    def get_prices(self, symbol, dtime):
        dtime = dtime.replace(second=0)
        df = self.__download_df(symbol)
        if df.empty:
            return 'NA', 'NA', 'NA', 'NA', 'NA'
        alert_price = self.__get_price(df, dtime)
        two_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=2))
        four_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=4))
        one_d_price = self.__get_price(df, dtime + datetime.timedelta(days=1))
        one_w_price = self.__get_price(df, dtime + datetime.timedelta(days=7))
        
        return alert_price, two_hr_price, four_hr_price, one_d_price, one_w_price
        

    def save_data(self):
        symbols = self.stocks_df.keys()
        for symbol in symbols:
            saveas = os.path.join(self.data_folder, f'{symbol}.csv')
            self.stocks_df[symbol].to_csv(saveas, index=False)
        logging.info(f"Stocks data saved to {self.data_folder}")

    def __get_price(self, df, dtime):
        if len(df.index) > 1:
            dates = pd.to_datetime(df['time']).dt.date.unique().tolist()
            start_date = df['time'].iloc[0]
            while True:
                value = df.loc[df['time'] == dtime, 'close'].values
                
                if (value.size > 0) or (dtime > start_date): 
                    break
                if dtime.date() not in dates:
                    dtime = dtime + datetime.timedelta(days=1)
                else:
                    dtime = dtime + datetime.timedelta(minutes=1)

            return value[0] if value.size > 0 else 'NA'   
        
        logging.debug("Empty dataframe")
        return 'NA'     

    def __download_df(self, symbol):
        download = False
        if symbol in self.stocks_df.keys():
            dfs = self.stocks_df[symbol]
            print(dfs)
            start_date = dfs['time'].iloc[0].to_pydatetime().date()
            today = datetime.date.today() - datetime.timedelta(days=1)
            if today > start_date:
                download = True
        else:
            self.stocks_df[symbol] = {}
            dfs = pd.DataFrame(columns=['time','open','high','low','close','volume'])
            download = True
            
        if download:
            logging.debug(f"{symbol}: New download")

            response = self.__get_response(symbol)
            if response:
                df = pd.read_csv(io.StringIO(response.text), sep=",")
                dfs = dfs.append(df, ignore_index = True)
                dfs['time'] =  pd.to_datetime(dfs['time']) 

                dfs.drop_duplicates(keep='first',inplace=True) 
                dfs = dfs.sort_values(by=['time'], ascending=False)
        
        if dfs.empty:
            return pd.DataFrame()
        self.stocks_df[symbol] = dfs
        return dfs

    def __get_response(self, symbol):
        API = self.api
        response = ''
        for i in range(5):
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=1min&slice=year1month1&apikey={API}'
            response = requests.get(url, timeout=60)
            if "Thank you for using Alpha Vantage" in response.text:
                logging.warning(f"No response from Alphavantage. Retrying {i+1}..")
            else:
                break
            time.sleep(15)
        
        if "Thank you for using Alpha Vantage" in response.text:
            logging.warning(f"No response from Alphavantage for {symbol}.")
            response = ''
        return response

if __name__ == "__main__":
    stk = StocksData('./data/stocks')

    sym = ['COHN', 'NETE', 'SNMP', 'UAMY', 'CARV', 'TIRX', 'PXS', 'ONE', 'CXDC']
    for s in sym:
        prices = stk.get_prices(s,datetime.datetime(2021, 8, 9, 13, 50, 33))
        print(s, prices)
    stk.save_data()
