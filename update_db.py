from datetime import datetime as dt
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
import requests
import json
import boto3
import psycopg2
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from NftTransaction import NftTransaction
import pandas
import api_tools as apit
import os
import time
import botocore


def _create_inserts():
    s3 = S3Hook('my_s3_conn')
    inserted_statements= []
    #initialize sql output file
    with open('/home/lilbean/airflow/dags/test.sql','w') as w:
        w.write("-- instatiating file")
    last_hour = (dt.utcnow() + datetime.timedelta(hours=-1)).strftime('%m_%d_%y_%H')
    collections = apit.collections
    # go thru every collection
    for collection in collections:
        bucket_name = f"beans-{collection}-data"
        prefix = f"{collection}_sales_data_{last_hour}"
        # iterate thru the redundant websocket event data
        for ws in range(3):
            fname = prefix + f'_{ws}.json'
            try:os.remove(f'/home/lilbean/data/{collection}/{fname}')
            except FileNotFoundError as e: pass
            print(fname)
            try:
                s3_response = s3.download_file(bucket_name=bucket_name,
                key = fname,
                local_path=f"/home/lilbean/data/{collection}",
                preserve_file_name=True,
                use_autogenerated_subdir=False
                )
            except botocore.exceptions.ClientError:
                continue
            f = open(f"/home/lilbean/data/{collection}/{fname}","r")
            try: data = json.load(f)
            except json.decoder.JSONDecodeError:
                continue

            # load the nft_id to token_name Data Frame
            nftDF = pandas.read_csv("/home/lilbean/airflow/dags/nft_ids.csv")

            events = data['data']
            transactions = {}

            # group events by tranasction
            tx_id = 0
            for event in events:
                payload = event['payload']['payload']

                tx_hash = payload['transaction']['hash']
                # check to if event belongs to a transaction thats known
                if tx_hash in transactions:
                    # add event to the NftTransaction object's list of events
                    # where is the NftTransaction is the value in a dict
                    # where the tx_hash is the key
                    transactions[tx_hash].events.append(event)
                else:
                    # create a NftTransaction object and add to dict
                    transactions[tx_hash] = NftTransaction(events = [event],
                    transaction_hash = tx_hash,
                    market_place = 'unknown',
                    tx_id = tx_id)
                    tx_id +=1



                    

            # go through each transaction
            for transaction in transactions.values():
                transaction.buyer_qty = {}
                transaction.events = apit.sort_events(transaction.events)           # go through tx event data and sort based on (buyer_address, token_id)
                first_event = transaction.events[0]
                payload = first_event['payload']['payload']
                slug = payload['collection']['slug']
                collection = apit.collection_slugs[slug]
                transaction.collection = collection
                transaction.token_ids = []
                transaction.sale_prices = {}
                

                #iterate over sorted events for each transaction
                for event in transaction.events:
                    payload = event['payload']['payload']
                    transaction.time_stamp=payload['event_timestamp'][:19]
                    block_number = apit.get_blocknumber(transaction.time_stamp)
                    transaction.block_number = block_number
                    token_id = payload['item']['token_id']
                    transaction.token_ids.append(token_id)
                    token_name = (collection, token_id)
                    transaction.parties[token_name] = [None, None]
                    if token_name not in transaction.sale_prices:
                        transaction.sale_prices[token_name] = 0

                

                    # if sale occurred on OpenSea
                    if(event['event'] == 'item_sold'):
                        transaction.sale_prices[token_name] = payload['sale_price']
                        if(transaction.market_place == 'unknown'):
                            transaction.market_place = 'opensea'
                        buyer = payload['taker']['address']
                        seller = payload['maker']['address']
                        continue


                    # else we have to figure out where it came from
                    # grab usable fields as API call parameters
                    elif(event['event'] == 'item_transferred'):
                        transaction.market_place= 'unknown'
                        seller = payload['from_account']['address']
                        buyer = payload['to_account']['address']
                        if(buyer not in transaction.buyer_qty):
                            transaction.buyer_qty[buyer] = 1
                        else:
                            # if they are, increment their transaction qty
                            transaction.buyer_qty[buyer] += 1
                        

                    # if buyer isn't recognized within this transaction
                    if(buyer !=transaction.parties[token_name][1]):
                        transaction.parties[token_name][1] = buyer
                    
                    # if seller isn't recognized within this transaction
                    if(seller != transaction.parties[token_name][0]):
                        transaction.parties[token_name][0] = seller

                

                    if(transaction.market_place == 'unknown'):
                        # look up buyer and seller of NFT to see if they initiated
                        # the Normal Transaction
                        normal_txs = apit.get_normal_txn(buyer, block_number=block_number, dump=True)
                        time.sleep(1)
                        # if buyer initiated
                        if normal_txs:
                            for txn in normal_txs:
                                # grab tx sale_price, market_place, and method_call 
                                if (txn['hash'] != transaction.transaction_hash):
                                    continue
                                transaction.nonce = txn['nonce']
                                transaction.method_call = txn['functionName'].split('(')[0]
                                checksum_to = apit.get_checksum_address(txn['to'])
                                try:
                                    market_place = apit.market_place[checksum_to]
                                except KeyError:
                                    market_place = 'unknown'
                                transaction.market_place=market_place
                                if(transaction.sale_prices[token_name] != 0):
                                    continue 
                                if(transaction.method_call == 'takeAsk'):
                                    normal_value = txn['value']
                                    continue
                                transaction.sale_prices[token_name] = txn['value']
                                transaction.init_address=buyer
                
                                

                        # seller initiated or blur-blend
                        else:
                            normal_txs = apit.get_normal_txn(seller, block_number=block_number, dump=True)
                            time.sleep(1)
                            if normal_txs:
                                # grab market_place, and method call
                                for txn in normal_txs:
                                    if (txn['hash'] != transaction.transaction_hash):
                                        continue
                                    checksum_to = apit.get_checksum_address(txn['to'])
                                    try:
                                        market_place = apit.market_place[checksum_to]
                                    except KeyError:
                                        market_place = 'unknown'
                                    transaction.market_place=market_place
                                    transaction.init_address=seller
                                    transaction.method_call = txn['functionName'].split('(')[0]
                                
                        # if any of these method_calls we know they are not valid sales
                        if(transaction.method_call == 'borrow'):
                            transaction.sale_prices[token_name] = -1
                        elif(transaction.method_call) == 'repay':
                            transaction.sale_prices[token_name] = -2
                        elif(transaction.method_call == 'batchRepayETH'):
                            transaction.sale_prices[token_name] = -2
                




                # if market_place is Blur's lending protocol
                if(transaction.market_place == 'blur-blend'):
                    if(transaction.method_call == 'takeBidV2'):
                        token_name = (transaction.collection, transaction.token_ids[0])
                        buyer = transaction.parties[token_name][1]
                        block_number = transaction.block_number
                        txn = apit.get_erc20_txns(buyer,block_number=block_number)[0]
                        transaction.sale_prices[token_name] = txn['value']
                    elif(transaction.method_call == 'buyToBorrowV2ETH'):
                        token_name = (transaction.collection, transaction.token_ids[0])
                        seller = transaction.parties[token_name][0]
                        block_number = transaction.block_number
                        txns = apit.get_int_txns(transaction.transaction_hash,dump=True)
                        for txn in txns:
                            if(txn['to'] != seller):
                                continue
                            transaction.sale_prices[token_name] = txn['value']
                    elif(transaction.method_call == 'buyLocked'):
                        token_name = (transaction.collection, transaction.token_ids[0])
                        buyer = transaction.parties[token_name][1]
                        block_number = transaction.block_number
                        txns = apit.get_erc20_txns(buyer,block_number, dump=True)
                        price = 0
                        for txn in txns:
                            checksum_from = apit.get_checksum_address(txn['from'])
                            if(checksum_from == buyer):
                                price = price + int(txn['value'])
                        transaction.sale_prices[token_name] = str(price)


                if(transaction.market_place == 'blur3'):
                    # Buyer initiated trade / sale_price is known
                    if(transaction.init_address == buyer):
                        if(transaction.method_call == 'takeAskSinglePool'):
                            txn_hash = transaction.transaction_hash
                            txns = apit.get_int_txns(txn_hash)
                            price = 0
                            seller = seller.lower()
                            for txn in txns:
                                lower_to = txn['to'].lower()
                                if(lower_to == seller):
                                    price = price + int(txn['value'])
                            transaction.sale_prices[token_name] = str(price)
                    else:
                    # Seller initiated trade
                        if(transaction.method_call == 'takeBidSingle'):
                            token_name = (transaction.collection, transaction.token_ids[0])
                            buyer = transaction.parties[token_name][1]
                            seller = apit.get_checksum_address(transaction.parties[token_name][0])
                            txns = apit.get_erc20_txns(buyer,transaction.block_number,dump=True)
                            price = 0
                            for txn in txns:
                                checksum_to = apit.get_checksum_address(txn['to'])
                                if(checksum_to == seller):
                                    price = price + int(txn['value'])
                            transaction.sale_prices[token_name] = str(price)
                            
                        else:    
                            market_address = apit.market_address[transaction.market_place]    
                            txns = apit.get_erc20_txns(market_address,
                            block_number=block_number, dump=True)
                            time.sleep(1)
                            for i in range(len(transaction.token_ids)):
                                token_name = (transaction.collection, transaction.token_ids[i])
                                buyer = transaction.parties[token_name][1]
                                for txn in txns:
                                    from_address = apit.get_checksum_address(txn['from'])
                                    check_price = transaction.sale_prices[token_name]
                                    if(check_price == 0 and buyer == from_address):
                                        transaction.sale_prices[token_name] = (
                                            str(int(int(txn['value']) / transaction.buyer_qty[buyer]))
                                        )
                                        break

                if(transaction.method_call == "takeAsk"):
                    for token_id in transaction.token_ids:
                        token_name = (transaction.collection, token_id)
                        buyer = transaction.parties[token_name][1]
                        transaction.sale_prices[token_name] = (
                            str(int(int(normal_value) / transaction.buyer_qty[buyer]))
                        )

                if(transaction.method_call == 'batchBuyWithERC20s'):
                    price = 0
                    while (True):
                        try:
                            for i,token_id in enumerate(transaction.token_ids):
                                try:
                                    buyer = transaction.parties[token_name][1]
                                    block_number = transaction.block_number
                                    txns = apit.get_erc20_txns(buyer, block_number=block_number)
                                    if(txns[:5] != 'Error'):
                                        break
                                except TypeError:
                                    pass
                            for txn in txns:
                                if(txn['nonce'] != transaction.nonce):
                                    continue
                                price = price + int(txn['value'])
                            for token_id in transaction.token_ids:
                                token_name = (transaction.collection, token_id)
                                transaction.sale_prices[token_name] = (
                                    str(int(int(price) / tranaction.buyer_qty[buyer]))
                                )
                            break
                        except TypeError:
                            time.sleep(60)
                            pass


                # print the sales
                transaction.printSales()
                # generate insert queries for valid sales
                transaction_queries, transaction_tuples = transaction.get_queries(nftDF)
                for i, insert in enumerate(transaction_queries):
                    insert_tuple = transaction_tuples[i]
                    # if this is an insert we havent seen
                    if insert_tuple not in inserted_statements:
                        # write to output sql file
                        with open('/home/lilbean/airflow/dags/test.sql', 'a') as t:
                            
                            t.write(insert)
                            inserted_statements.append(insert_tuple)
      
    
    

    



with DAG(
        'update_db_dag',
        start_date=dt(2023,5,9),
        schedule_interval="7 */1 * * *",
        catchup=False
    ) as dag:
        create_inserts = PythonOperator(
        task_id = "create_inserts",
        python_callable=_create_inserts,
        dag=dag
        )
        
        update_db = PostgresOperator(
        task_id='update_db',
        sql='/test.sql',
        postgres_conn_id='postgres_tsdb',
        dag=dag)
        


        ready = DummyOperator(task_id='ready')


        create_inserts >> update_db >> ready



