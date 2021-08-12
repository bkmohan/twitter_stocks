import tweepy, datetime, csv
from crypto import CryptosData
from stocks import StocksData
import logging

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

#### Tweepy credentials
consumer_key = 'iSaQ7guMk4kkgYIaUwA31zLZo'
consumer_secret = 'CYMbWNYCJ35GwZIqxp0ccpirmZs7COyEQCImvyNRMLyQjr2WjH'

def get_twitter_ids():
    # ids = ['mitch___picks','pennystocksmom','pamp3rz','crypt0king07','reysantoscrypto']
    ids = ['Michael___hunt']
    return ids

def date_delta(days):
    today = datetime.date.today()
    diff = today - datetime.timedelta(days=days)
    return diff

def preffered_ticker(symbols, crypto_symbols):
    check_symbols = set()

    preffer = 0
    for i in range(len(symbols)):
        if symbols[i]['Symbol'] not in check_symbols: check_symbols.add(symbols[i]['Symbol'])
        if symbols[i]['Symbol'] in crypto_symbols:  preffer += 1
        if len(check_symbols) >= 10: break

    return 'Cryptos' if preffer >= 7 else 'Stocks' 

def write_csv(d, prices):
    row = {
                'Username' : d['ID'],
                'Date' : d['Date'].date(),
                'Time' : d['Date'].time(),
                'CashTag' : d['Symbol'],
                'Alert Price' : prices[0],
                '2hr' : prices[1],
                '4hr' : prices[2],
                '1D' : prices[3],
                '1w' : prices[4], 
                'Tweet': d['Tweet']
            }
    filename = 'Tweets_prices.csv'
    field_names = ['Username', 'Date', 'Time', 'CashTag', 'Alert Price', '2hr', '4hr', '1D', '1w', 'Tweet']
    with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writerow(row)

    logging.info(f"{d['ID']}: {row}. written to csv")

def create_csv():
    filename = 'Tweets_prices.csv'
    field_names = ['Username', 'Date', 'Time', 'CashTag', 'Alert Price', '2hr', '4hr', '1D', '1w', 'Tweet']
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
 

def get_prices(data, pref):
    if pref == 'Stocks':
        for d in data:
            prices = stock_obj.get_prices(d['Symbol'], d['Date'])
            if prices == ('NA', 'NA', 'NA', 'NA', 'NA'):
                prices = crypto_obj.get_prices(d['Symbol'], d['Date'])
            write_csv(d, prices)
    else:
        for d in data:
            prices = crypto_obj.get_prices(d['Symbol'], d['Date'])
            if prices == ('NA', 'NA', 'NA', 'NA', 'NA'):
                prices = stock_obj.get_prices(d['Symbol'], d['Date'])
            write_csv(d, prices)
        
    

def tweets_data(id, days=30):
    tweet_data = []
    delta = date_delta(days)

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
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth)

    stock_obj = StocksData('./data/stocks')
    crypto_obj = CryptosData('./data/cryptos')
    crypto_symbols = crypto_obj.get_cryptos()

    create_csv()
    ids = get_twitter_ids()
    for id in ids:
        data = tweets_data(id, days=30)
        pref = preffered_ticker(data, crypto_symbols)
        get_prices(data, pref)

    stock_obj.save_data()
    crypto_obj.save_data()



