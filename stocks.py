import os, glob, datetime, requests, io, logging, time, json
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

class AlphavantageAPI():
    """
    Alphavantage API proivdes from list of given APIs.
    Alphavantage API has a limit of 5 calls per minute and 500 calls per day.
    This class regulates the limits and iterates over provided API list.
    
    Methods
    -------
    get_api()
        Returns Alphavantage API
    """


    def __init__(self, api_list:list):
        """
        Initiantes AlphavantageAPI API, with list of api in api_list
        
        Parameters
        ----------
        arg1 : list
            List of valid Alphavantage APIS
        """
        self.apis_iterator = iter(api_list)
        self.api = next(self.apis_iterator)
        self.curr_count = 0
        self.limit = 450

    def get_api(self):
        """
        Returbs current API, also regulates the API limit and iterates to next if required.

        Returns
        -------
        str
            Alphavantage API
        """
        if self.curr_count <= self.limit:
            return self.api
        else:
            try:
                self.api = next(self.apis_iterator)
                return self.api
            except StopIteration:
                return 'NA'

    # def reset_apis(self):
    #     self.curr_count = 0


class StocksData():
    """
    StocksData class gets all relvent stocks prices for a given symbol and datetime object.
    
    Methods
    -------
    get_prices(symbol, datetime)
        Returns tuple of prices (Alert Price, 2hr, 4hr, 1D, 1W)

    save_data()
        saves all stocks dataframe downladed to (data_folder) folder
    """

    def __init__(self, data_folder, API_List, IEX_API):
        """
        Initiantes StocksData class with (data_folder) path for saving downloaded stock data and 
        (API_List) a list ot Alphavantage APIs
        
        Parameters
        ----------
        arg1 : str
            (data_folder) Folder path for saving downloaded files
        arg2 : list
            (APIs) List of Alphavantage APis
        arg3 : str:
            IEXCloud Api for getting current price
        """
        self.data_folder = data_folder
        self.IEX_API = IEX_API
        all_files = glob.glob(os.path.join(data_folder, "*.csv"))
        self.stocks_df = {}

        logging.debug("Fetching Alphavantage APIs")
        
        self.apis =  AlphavantageAPI(API_List)
        
        logging.info(f"Reading stocks csv from {data_folder}")
        for file in all_files:
            file_name = os.path.splitext(os.path.basename(file))[0]
            dfn = pd.read_csv(file)
            dfn['time'] =  pd.to_datetime(dfn['time'])
            self.stocks_df[file_name] = dfn

    def get_prices(self, symbol, dtime):
        """
        Returns tuple of prices for the stock (symbol) and dtime as Alert time (Alert Price, 2hr, 4hr, 1D, 1W)
        
        Parameters
        ----------
        arg1 : str
            Stock Symbol
        arg2 : datetime
            Alert or created datetime object
        Returns
        -------
        tuple
            Returns tuple of prices (Alert Price, 2hr, 4hr, 1D, 1W)
        """
        dtime = dtime.replace(second=0)
        symbol = symbol.upper()
        df = self.__download_df(symbol)
        if df.empty:
            ''' Return NA if no stock found in Alphavnatage datbase'''
            return 'NA', 'NA', 'NA', 'NA', 'NA', 'NA'

        alert_price = self.__get_price(df, dtime)
        two_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=2))
        four_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=4))
        one_d_price = self.__get_price(df, dtime + datetime.timedelta(days=1))
        one_w_price = self.__get_price(df, dtime + datetime.timedelta(days=7))
        current_price = self.__get_current_price(symbol)

        return alert_price, two_hr_price, four_hr_price, one_d_price, one_w_price, current_price
        

    def save_data(self):
        """
        Saves all downloaded data into local machine (path=self.data_folder)
        """
        symbols = self.stocks_df.keys()
        for symbol in symbols:
            saveas = os.path.join(self.data_folder, f'{symbol}.csv')
            self.stocks_df[symbol].to_csv(saveas, index=False)
        logging.info(f"Stocks data saved to {self.data_folder}")

    def __save_symbol(self, df, symbol):
        """
        Saves individial downloaded data of stock(symbol)into local machine (path=self.data_folder)
        
        Parameters
        ----------
        arg1 : Pandas Dataframe
            df, stock's downloaded data
        arg2 : str
            =Stock's symbol
        """
        saveas = os.path.join(self.data_folder, f'{symbol}.csv')
        df.to_csv(saveas, index=False)
        logging.info(f"{symbol} data saved to {self.data_folder}")

    def __get_current_price(self, symbol):
        """
        Method returns current price of the stock from IEXcloud server
        
        Parameters
        ----------
        arg1 : str
            Stock's symbol

        Return
        ------
            float
                Current price of the stock (symbol)
        """
        url = f'https://cloud.iexapis.com/stable/stock/{symbol}/quote?token={self.IEX_API}'
  
        response = requests.get(url, timeout=30).json()
        return response['iexClose']

    def __get_price(self, df, dtime):
        """
        Private method returns price of the stock at particular date-time(dtime)
        
        Parameters
        ----------
        arg1 : Pandas Dataframe
            Dataframe of the stock
        arg2 : datetime
            datetime of the price requires
        Returns
        -------
        float
            Returns price of exist 
        str
            Return NA if no price found for particlar time
        """
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
        """
        Private method to downloads data from Aplhavantage database for the stock(symbol)
        
        Parameters
        ----------
        arg1 : str
            Stock Symbol
    
        Returns
        -------
        Pandas DataFrame
            downloaded data as dataframe
        """
        download = False
        if symbol in self.stocks_df.keys():
            dfs = self.stocks_df[symbol]
            
            start_date = dfs['time'].iloc[0].to_pydatetime().date()
            today = datetime.date.today() - datetime.timedelta(days=1)
            if today > start_date:
                download = True
        else:
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
                    
        if not dfs.empty:
            self.stocks_df[symbol] = dfs
            self.__save_symbol(dfs, symbol)
            return dfs
        return dfs

    def __get_response(self, symbol):
        """
        Returns the response from Alphavantage.
        As Alphavantage limits to 5 calls per minute, the method retries 5 time with 
        delay of 15s if no response from Alphavatage server
        
        Parameters
        ----------
        arg1 : str
            Stock Symbol
    
        Returns
        -------
        Requests response
            response object from the requests made to Alphavantage API
        """
        API = self.apis.get_api()
        if API == "NA":
            logging.critical("Alphavantage API Exhausted.")
            return ''
        response = ''
        logging.info(f"Downloading data of stock: {symbol}")
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
    config = json.load(open(r'D:\Scrappers\Anurag\Twitter\code\config.json'))
    stk = StocksData(r'D:\Scrappers\Anurag\Twitter\code/data/stocks', config['alphavantage_apis'], config['iexcloud_api'])

    sym = ['COHN', 'NETE', 'SNMP', 'UAMY', 'CARV', 'TIRX', 'PXS', 'ONE', 'CXDC']
    for s in sym:
        prices = stk.get_prices(s,datetime.datetime(2021, 8, 9, 13, 50, 33))
        print(s, prices)
    stk.save_data()
