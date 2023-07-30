import time
import pandas
import api_tools as apit
from datetime import date
import datetime
class NftTransaction:
    '''
    Purpose:
        To model an NFT transaction
    
    Attributes:
        events  (list) : A list of JSON objects holding event data
        parties (dict) : key = (collection,token_id) 
                         value = (seller address,buyer address)

        block_number (int) : Ethereum confirmation block
        time_stamp   (str) : time the block was confirmed

        market_place (str) : platform where the transaction originated
        token_ids    (list): ids for the NFTs transferred
        collection   (str) : The symbol for the NFT collection
        transaction_hash (str) : output hash of the transaction confirmation
        sales_prices (dict) : key = (collection,token_id) 
                              value = sale_price
        buyer_qty    (dict) : key = buyer_address
                              value = number of NFTs purchased in transaction
        init_address (str)  : the address that initiated the Normal transaction
        method_call  (str)  : the method that was called to initiate transaction
        tx_id        (int)  : the Nth transaction within hourly batched event data

        
    '''
    def __init__(self,
        events = [],
        parties = {},
        block_number = None,
        time_stamp = None,
        market_place = None,
        token_ids = [],
        collection = None,
        transaction_hash = None,
        sale_prices = {},
        buyer_qty = {},
        init_address = None,
        method_call = None,
        tx_id = None,
        nonce= None):
        self.events = events
        self.parties = parties
        self.block_number = block_number
        self.time_stamp = time_stamp
        self.market_place = market_place
        self.token_ids = token_ids
        self.collection = collection
        self.transaction_hash = transaction_hash
        self.sale_prices = sale_prices
        self.buyer_qty = buyer_qty
        self.init_address = init_address
        self.method_call = method_call
        self.nonce = nonce


    def printTransaction(self):
        '''
        Description:
            output all the important transaction data
        '''
        print(f"""
        collection : {self.collection}
        token_ids  : {self.token_ids}
        block #    : {self.block_number}
        parties    : {self.parties}
        buyer_qty  : {self.buyer_qty}
        prices     : {self.sale_prices}
        market     : {self.market_place}
        """)

    def printSales(self):
        '''
        Description:
            print sale transaction data
        '''
        print(f"{self.market_place} : {self.sale_prices}")
        print(f"method call : {self.method_call}")
        print(f"txn hash : {self.transaction_hash}")

        time.sleep(2)

    def get_queries(self, nftDF):
        '''
        Description:
            takes in sale data and returns sql update query

        Parameters:
            collection      (string)
            token_id        (string)
            price           (string)
            market_place    (string)
            ts              (timestamp)
            block_number    (string)
            

        Returns
            update_query (string)
        '''
        insert_queries = []
        insert_tuples = []
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for token_id in self.token_ids:
            token_name = (f"{self.collection} {token_id}")
            nft_id = int(nftDF.loc[nftDF['token_name'] == token_name, 'nft_id'].values[0])
            token_name = (self.collection, token_id)
            price = str(self.sale_prices[token_name])
            if(int(price) <= 0):
                continue
            price = price[:-15]
            market_id = apit.market_id[self.market_place]
            date_id = self.time_stamp[:10]
            date_id = date.fromisoformat(date_id)
            date_id = date.strftime(date_id, '%Y%m%d')
            buyer = self.parties[token_name][1]
            seller = self.parties[token_name][0]
            insert_query = f'''
            INSERT INTO f_transaction
            (nft_id,market_id,date_id,block_id,buyer,seller,txn_hash, sale_price, updated_at)
            VALUES
            ({nft_id},{market_id},{date_id},{self.block_number},
            '{buyer}',
            '{seller}',
            '{self.transaction_hash}',
            {price},'{now}');
            '''
            insert_tuples.append((nft_id,market_id,self.block_number, self.transaction_hash, price))
            insert_queries.append(insert_query)

        return insert_queries, insert_tuples

    
   