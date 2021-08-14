import tweepy, datetime, csv, json
from py.crypto import CryptosData
from py.stocks import StocksData
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def get_twitter_ids(ids_filename):
    """
    Returns Twitter user ids read from the filename: ids_filename
    Skips the first row as header, twitter ids are obtained from 1st Column and 2nd rows [A2:A]
    
    Parameters
    ----------
    arg1 : str
        Input twitter id's filename

    Returns
    -------
    list
        list of twitter user ids in first column of the *.csv file
    """
    logging.info(f"Reading Twitter ids from {ids_filename}")
    ids = []
    with open(ids_filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            ids.append(row[0])
    return ids

def date_delta(days):
    """
    Returns date of (today - days)
    
    Parameters
    ----------
    arg1 : int
        number of days

    Returns
    -------
    datetime
        datetime object of (today - days)'s date
    """
    today = datetime.date.today()
    diff = today - datetime.timedelta(days=days)
    return diff

def preffered_ticker(tweet_data, crypto_symbols):
    """
    Returns preffred ticker between Stock and Crypto.
    Checks presence of first 10 unique symbols from the tweets in list of all cryptos.
    If there are 70% match, then preffered ticker is Crypto else Stock.
    
    Parameters
    ----------
    arg1 : list
        list of tweet_data
    arg2 : list:
        list of all cruptos
    Returns
    -------
    str
        Preffered Ticker
    """
    check_symbols = set()
    crypto_ids = []
    for crypto in crypto_symbols:
        crypto_ids.append(crypto['symbol'])

    preffer = 0
    for i in range(len(tweet_data)):
        if tweet_data[i]['Symbol'] not in check_symbols: check_symbols.add(tweet_data[i]['Symbol'])
        if tweet_data[i]['Symbol'] in crypto_ids:  preffer += 1
        if len(check_symbols) >= 10: break

    return 'Cryptos' if preffer >= 7 else 'Stocks' 

def write_csv(output, tweet_data, prices):
    """
    Writes Tweet data and it's prices to a row of output(csv) file
    
    Parameters
    ----------
    arg1 : filename/path
        output filename
    arg2 : dict
        single tweet data
    arg3 : tuple
        tuple of prices (Alert Price, 2hr, 4hr, 1D, 1W, Current Price)
    """
    row = {
                'Username' : tweet_data['ID'],
                'Date' : tweet_data['Date'].date(),
                'Time' : tweet_data['Date'].time(),
                'CashTag' : tweet_data['Symbol'],
                'Alert Price' : prices[0],
                '2hr' : prices[1],
                '4hr' : prices[2],
                '1D' : prices[3],
                '1w' : prices[4], 
                'Current Price' : prices[5],
                'Tweet': tweet_data['Tweet']
            }
    field_names = ['Username', 'Date', 'Time', 'CashTag', 'Alert Price', '2hr', '4hr', '1D', '1w', 'Current Price' ,'Tweet']
    with open(output, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writerow(row)

    logging.info(f"Exporting data: {tweet_data['ID']}: {row}.")

def create_csv(output):
    """
    Creates new output csv file, with headers ('Username', 'Date', 'Time', 'CashTag', 'Alert Price', '2hr', '4hr', '1D', '1w', 'Current Price', 'Tweet')
    
    Parameters
    ----------
    arg1 : str
        output filename
    """
    field_names = ['Username', 'Date', 'Time', 'CashTag', 'Alert Price', '2hr', '4hr', '1D', '1w', 'Current Price', 'Tweet']
    with open(output, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
 

def get_prices(output, tweet_data, pref):
    """
    Gets all prices for the symbols in list of tweet data for the preffered(pref) ticker.
    Writes the data to ouptut csv.
    
    Parameters
    ----------
    arg1 : str
        Output filepath
    arg2 : list
        list of tweet_data
    arg3 : str
        prefferd ticker (Stocks or Cryptos)
    """
    if pref == 'Stocks':
        for d in tweet_data:
            prices = stock_obj.get_prices(d['Symbol'], d['Date'])
            if prices == ('NA', 'NA', 'NA', 'NA', 'NA', 'NA'):
                prices = crypto_obj.get_prices(d['Symbol'], d['Date'])
            write_csv(output, d, prices)
    else:
        for d in tweet_data:
            prices = crypto_obj.get_prices(d['Symbol'], d['Date'])
            if prices == ('NA', 'NA', 'NA', 'NA', 'NA', 'NA'):
                prices = stock_obj.get_prices(d['Symbol'], d['Date'])
            write_csv(output, d, prices)
        
    

def tweets_data(id, days=30):
    """
    Gets tweets for the Twitter id(id) for previous days (default=30) up to previous 3000 tweets.
    
    Parameters
    ----------
    arg1 : str
        Twitter Id
    arg2 : int
        upto days (default=30)
    Returns
    -------
    list
        list of tweets data, each data constitues (User ID, Symbol in tweet, Date of Tweet, Tweet text)
    """
    tweet_data = []
    delta = date_delta(days)

    logging.info(f"Fetching tweets of ID: {id}")
    for tweet in tweepy.Cursor(api.user_timeline, id=id).items():
        text = tweet.text
        date = tweet.created_at
        if date.date() < delta:
            break
        for symbol in tweet.entities['symbols']:
            tweet_data.append({
                'ID' : id, 
                'Symbol' : symbol['text'], 
                'Date' : date, 
                'Tweet' : text
            })

    return tweet_data



if __name__ == '__main__':

    config = json.load(open('config.json')) # predefined params

    consumer_key = config['tweepy_consumer_key']
    consumer_secret = config['tweepy_consumer_secret']
    
    # Login in to tweepy API
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth)

    stock_obj = StocksData('./data/stocks', config['alphavantage_apis'], config['iexcloud_api'])
    crypto_obj = CryptosData('./data/cryptos')
    crypto_symbols = crypto_obj.get_cryptos()

    output = config['output_filename']
    create_csv(output)
    ids = get_twitter_ids(config['twiiter_ids_input'])
    for id in ids:
        data = tweets_data(id, days=30)
        pref = preffered_ticker(data, crypto_symbols)
        get_prices(output, data, pref)

    stock_obj.save_data()
    crypto_obj.save_data()



