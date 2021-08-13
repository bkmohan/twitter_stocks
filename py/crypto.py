import os, glob, datetime, requests, logging, time
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

class CryptosData():
    """
    CryptosData class gets all relvent cryptos prices for a given symbol and datetime object.
    
    Methods
    -------
    get_prices(symbol, datetime)
        Returns tuple of prices (Alert Price, 2hr, 4hr, 1D, 1W)

    save_data()
        saves all crypros dataframe downladed to (data_folder) folder
    """

    def __init__(self, data_folder):
        """
        Instantiates CryptosData class with (data_folder) path for saving downloaded crypto data and 
        
        Parameters
        ----------
        arg1 : str
            (data_folder) Folder path for saving downloaded files
               
        """
        self.data_folder = data_folder
        all_files = glob.glob(os.path.join(data_folder, "*.csv"))
        self.cryptos_df = {}
        self.__init_cryptos()

        logging.info(f"Reading cryptos csv from {data_folder}")
        for file in all_files:
            file_name = os.path.splitext(os.path.basename(file))[0]
            dfn = pd.read_csv(file)
            dfn['time'] =  pd.to_datetime(dfn['time'])
            self.cryptos_df[file_name] = dfn

    def get_prices(self, symbol, dtime):
        """
        Returns tuple of prices for the crypto (symbol) and dtime as Alert time (Alert Price, 2hr, 4hr, 1D, 1W)
        
        Parameters
        ----------
        arg1 : str
            crypto Symbol
        arg2 : datetime
            Alert or created datetime object
        Returns
        -------
        tuple
            Returns tuple of prices (Alert Price, 2hr, 4hr, 1D, 1W)
        """
        dtime = dtime.replace(second=0)
        df = self.__download_df(symbol)

        alert_price = self.__get_price(df, dtime)
        two_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=2))
        four_hr_price = self.__get_price(df, dtime + datetime.timedelta(hours=4))
        one_d_price = self.__get_price(df, dtime + datetime.timedelta(days=1))
        one_w_price = self.__get_price(df, dtime + datetime.timedelta(days=7))
        
        return alert_price, two_hr_price, four_hr_price, one_d_price, one_w_price

    def save_data(self):
        """
        Saves all downloaded data into local machine (path=self.data_folder)
        """
        symbols = self.cryptos_df.keys()
        for symbol in symbols:
            saveas = os.path.join(self.data_folder, f'{symbol}.csv')
            self.cryptos_df[symbol].to_csv(saveas, index=False)
        logging.info(f"Cryptos data saved to {self.data_folder}")

    def __save_symbol(self, df, symbol):
        """
        Saves individial downloaded data of Crypto(symbol)into local machine (path=self.data_folder)
        
        Parameters
        ----------
        arg1 : Pandas Dataframe
            df, Crypto's downloaded data
        arg2 : str
            =Crypto's symbol
        """
        saveas = os.path.join(self.data_folder, f'{symbol}.csv')
        df.to_csv(saveas, index=False)
        logging.info(f"{symbol} data saved to {self.data_folder}")

    def __get_price(self, df, dtime):
        """
        Private method returns price of the crypto at particular date-time(dtime)
        
        Parameters
        ----------
        arg1 : Pandas Dataframe
            Dataframe of the crypto
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
                value = df.loc[df['time'] == dtime, 'price'].values
                
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
        Private downloads data from Coingecko database for the crypto(symbol)
        
        Parameters
        ----------
        arg1 : str
            crypto Symbol
    
        Returns
        -------
        Pandas DataFrame
            downloaded data as dataframe
        """
        download = False
        if symbol in self.cryptos_df.keys():
            dfs = self.cryptos_df[symbol]
            print(dfs)
            start_date = dfs['time'].iloc[0].to_pydatetime().date()
            today = datetime.date.today() - datetime.timedelta(days=1)
            if today > start_date:
                download = True
        else:
            dfs = pd.DataFrame(columns=['time','price'])
            download = True
            
        if download:
            logging.debug(f"{symbol}: New download")

            response = self.__get_response(symbol)
            if response:
                data = response.json()['prices']
                df = pd.DataFrame(data, columns=['time', 'price'])
                df['time'] =  pd.to_datetime(df['time'], unit='ms') 
                df['time']= df['time'].dt.round('min')
                dfs = dfs.append(df, ignore_index = True)
                
                dfs.drop_duplicates(keep='first',inplace=True) 
                dfs = dfs.sort_values(by=['time'], ascending=False)
        
        if not dfs.empty:
            self.cryptos_df[symbol] = dfs
            self.__save_symbol(dfs, symbol)
            return dfs
        return dfs

    def __get_response(self, symbol):
        """
        Returns the response from Coingecko.
        
        Parameters
        ----------
        arg1 : str
            Crypto Symbol
    
        Returns
        -------
        Requests response
            response object from the requests made to Coingeck0 API
        """
        id = ''
        for crypto in self.get_cryptos():
            if crypto['symbol'].lower() == symbol.lower():
                id = crypto['id']
                break
        if id:
            response = ''
            for i in range(5):
                try:
                    logging.info(f"Downloading data of crypto: {symbol}")
                    url = f'https://api.coingecko.com/api/v3/coins/{id}/market_chart?vs_currency=usd&days=30&interval=hourly'
                    response = requests.get(url, timeout=60)
                    return response
                except Exception as e:
                    logging.error(e)
                time.sleep(5)
        
        
        logging.warning(f"No response for {symbol} from coingecko.")
        return ''

    def __init_cryptos(self):
        """
        Downloads all cryptos listed in Coingecko website
        """
        url = f'https://api.coingecko.com/api/v3/coins/list'
        response = requests.get(url, timeout=30)
        self.all_cryptos = response.json()

    def get_cryptos(self):
        """
        Returns all cryptos listed in Coingecko API

        Returns
        -------
        dict
            Coingecko cryptos dict
        """
        return self.all_cryptos

if __name__ == "__main__":
    stk = CryptosData('./data/cryptos')

    sym = ['btc', 'eth', 'cat', 'bcna', 'axn', 'klee', 'pgo', 'min']
    for s in sym:
        prices = stk.get_prices(s,datetime.datetime(2021, 8, 9, 13, 50, 33))
        print(s, prices)
    stk.save_data()
