# twitter_stocks
Install requirents by running command
 > pip install -r requirements.py

Run twiiter_stocks script by command
 > py tweets.py
  
Update the fields in config.json file before running the script:
  1. tweepy_consumer_key = Twitter developer account's Consumer Key
  2. tweepy_consumer_secret = Twitter developer account's Secret
  3. alphavantage_apis = List of Alphavantage APIs
  4. iexcloud_api = IEX cloud API
  5. twiiter_ids_input = Path to csv file containing Twiiter UserIds
  6. output_filename = Outfile filename
  
 Note:
  Remove sample csv files from ./data/stock and ./data/crypto folders.
