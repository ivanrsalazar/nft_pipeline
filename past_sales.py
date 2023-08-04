import requests
import api_tools as apit
from NftTransaction import NftTransaction
import time
import pandas
import psycopg2
import os

db_password = os.environ.get('db_pass')


db_name = 'initial_beans'
db_user = 'postgres'
db_host = 'beansdata.cy28vrcdxihx.us-east-2.rds.amazonaws.com'  # Use 'localhost' if the database is on the same machine
db_port = '5432'  # Usually 5432 for PostgreSQL

connection = psycopg2.connect(
        database=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )


cursor = connection.cursor()    

#-------------------------------------------------------------------------------
#---------------------------READ THIS-------------------------------------------
# uncomment one of the lines below to select which collection fill-in sales for
#collection = 'BAYC'; token_range = 10_000
collection = 'MAYC'; token_range = 30_008
#collection = 'BAKC'; token_range = 10_000
#collection = 'AZUKI'; token_range = 10_000


#-------------------------------------------------------------------------------
#---------------------------READ THIS-------------------------------------------
# You need to import the surrogate primary keys for each NFT within your data warehouse
nftDF = pandas.read_csv("/Users/ivansalazar/Desktop/sandbox/sandbox/sandbox/nft_ids.csv")
for token_id in range(579,token_range,1):
    print(f'token_id : {token_id}')
    token_sales = apit.get_nft_sales(collection, token_id, dump=True)
    for sale in token_sales:
        block_number = int(sale['blockNumber'])
        ts = apit.get_blocktime(block_number, isString=True)
        total_price = 0
        try:total_price += int(sale['sellerFee']['amount'])
        except KeyError: pass
        try:total_price += int(sale['protocolFee']['amount'])
        except KeyError: pass
        try:total_price += int(sale['royaltyFee']['amount'])
        except KeyError: pass
        if(total_price <= 0):
            continue
        checksum_market = apit.get_checksum_address(sale['marketplaceAddress'])
        token_name = (collection, token_id)
        current_transaction = NftTransaction(
        parties = {token_name:[sale['sellerAddress'],sale['buyerAddress']]},
        block_number = block_number,
        time_stamp = ts,
        token_ids = [token_id],
        collection =  collection,
        transaction_hash = sale['transactionHash'],
        sale_prices = {token_name:total_price},
        buyer_qty = {},
        market_place = apit.market_place[checksum_market]
        )

        queries_list, insert_tuple = current_transaction.get_queries(nftDF)
        if(insert_tuple[0][4] == ''):
            continue
        exists_query = f"""select count(*) from f_transaction
                            where nft_id = {insert_tuple[0][0]}
                            and block_id = {insert_tuple[0][2]}
                            and txn_hash = '{insert_tuple[0][3]}';"""
        cursor.execute(exists_query)
        count = cursor.fetchone()[0]
        if count > 0:
            print("continue")
            continue
        try:
            cursor.execute(queries_list[0])
            connection.commit()
        except psycopg2.errors.SyntaxError:
            print(insert_tuple[0])
        print('insert executed')
    time.sleep(1)
        


cursor.close()
connection.close()
        



