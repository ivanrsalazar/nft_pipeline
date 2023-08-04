import requests
import json
import datetime
import time
from requests.adapters import HTTPAdapter, Retry
from Crypto.Hash import keccak
import os

'''
method_names = [
    'takeBidSingle' : 'Blur3',
    'takeAskSingle' : 'Blur3',
    'takeBid'       : 'Blur3',
    'takeAsk'       : 'Blur3',
    ''
    'executeMultipleTakerBids' : 'LooksRare',
    'executeTakerAsk' : 'LooksRare',
    'executeTakerBid' : 'LooksRare',
    'Execute'         : 'LooksAgg',
    'run' : 'X2Y2',
    'batchBuyWithERC20s' : 'Blur2'
    '0x9a2b8115'    : 'Blur2'                                                   # batchBuyWithETH
    'bulkExecute' : 'Blur'
    'execute'     : 'Blur'
    'takeBidV2'   : 'Blur: Blend'
]
'''


# dictionary for extacting collection symbol from collection slug
collection_slugs = {
    'mutant-ape-yacht-club': 'MAYC',
    'boredapeyachtclub' : 'BAYC',
    'bored-ape-kennel-club' : 'BAKC',
    'azuki' : "AZUKI"
    }

collections = ['bayc', 'mayc', 'bakc','azuki']   

collection_contract = {
    'bayc' : '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D',
    'mayc' : '0x60E4d786628Fea6478F785A6d7e704777c86a7c6',
    'bakc' : '0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623',
    'azuki' : '0xED5AF388653567Af2F388E6224dC7C4b3241C544'
}

# dictionary for extracting marketplace from marketplace contract address
# CheckSum address
market_place = {
    '0x00000000006c3852cbEf3e08E8dF289169EdE581' : 'opensea',
    '0x00000000000000ADc04C56Bf30aC9d3c0aAF14dC' : 'opensea',
    '0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b' : 'opensea',
    '0x7f268357A8c2552623316e2562D90e642bB538E5' : 'opensea',
    '0x00000000000001ad428e4906aE43D8F9852d0dD6' : 'opensea',
    '0x29469395eAf6f95920E59F858042f0e28D98a20B' : 'blur-blend',
    '0xb2ecfE4E4D61f8790bbb9DE2D1259B9e2410CEA5' : 'blur3',
    '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3' : 'x2y2',
    '0x0000000000E655fAe4d56241588680F86E3b2377' : 'looks',
    '0x59728544B08AB483533076417FbBB2fD0B17CE3a' : 'looks',
    '0x00000000005228B791a99a61f36A130d50600106' : 'looks-agg',
    '0x39da41747a83aeE658334415666f3EF92DD0D541' : 'blur2',
    '0x000000000000Ad05Ccc4F10045630fb830B95127' : 'blur',
    '0x9Da9571389BA2316ad98f695210aD5fB0363cDEd' : 'bend',
    '0x60E4d786628Fea6478F785A6d7e704777c86a7c6' : 'mayc'
}

royalty_wallets = {
    '0xB4D24DAcbdFfA1BBf9A624044484b3FEeB7fdF74' : "azuki-royalties"
}

# dictionary for extracting marketplace from marketplace contract address
# blur2 looks to be blur-agggregator
market_address = {
    'blur-blend' : '0x29469395eaf6f95920e59f858042f0e28d98a20b',
    'blur3' : '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5',
    'x2y2' : '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3',
    'looks' : '0x0000000000E655fAe4d56241588680F86E3b2377',
    'opensea' : '0x00000000000000ADc04C56Bf30aC9d3c0aAF14dC',
    'looks-agg' : '0x00000000005228B791a99a61f36A130d50600106',
    'blur2' : '0x39da41747a83aeE658334415666f3EF92DD0D541',
}

market_id = {
    'opensea' : '1',
    'x2y2' : '2',
    'looks': '3',
    'blur3' : '4',
    'blur'  : '4',
    'blur-blend' : '5',
    'bend' : '6',
    'looks-agg' : '7',
    'unknown' : '999'
}

def get_nft_sales(collection,token_id,block_number=None,timestamp=None,
    dump=False): 
    '''
    Description: 
        takes in NFT data and block_number or block confirmation timestamp
        and returns with a list of sales dictionaries with sale data

    Parameter:
        collection   (string)
        token_id     (integer)
        block_number (integer)
        timestamp    (timestamp)
    
    Returns:
        sale_data (a list of dictionaries)
    
    '''

    
    alc_key = os.environ.get('alc_key')
    url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{alc_key}/getNFTSales" 


    headers = {"accept": "application/json"}
    params = {
    'order' : 'asc',
    'fromBlock' : '0',
    'toBlock' : 'latest'
    }

    params['contractAddress'] = collection_contract[collection.lower()]
    params['tokenId'] = str(token_id)
    if(timestamp):
        block_number = est.get_blocknumber(timestamp)
    params['fromBlock'] = block_number
    params['toBlock'] = block_number

    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=5, total = 10)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url, params=params, headers=headers)
    json_response = json.loads(response.text)
    if(dump):
        with open("nft_sales.json", 'w') as f:
            json.dump(json_response,f,indent=4)
    sale_data = json_response['nftSales']
    return sale_data


def get_blocktime(block_number, isString=False):
    '''
    Description: 
        returns the UTC timestamp at which the block was confirmed

    Parameter:
        block_number (integer)
    
    Returns:
        UTC timestamp (timestamp)
    
    '''

    # when done with documentation, change to environment variablex
    alc_key = os.environ.get('alc_key')
    url=f"https://eth-mainnet.g.alchemy.com/v2/{alc_key}"

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [str(hex(block_number)), True]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.post(url, json=payload, headers=headers)
    json_response = json.loads(response.text)
    unix_time = int(json_response['result']['timestamp'],16)
    utc_time = datetime.datetime.utcfromtimestamp(unix_time)
    if(isString):
        utc_time = utc_time.strftime("%Y-%m-%d %H:%M:%S.000000+00:00")

    return utc_time

def get_blockdata(block_number):
    '''
    Description:
        returns a dictionary with data about the ethereum block
        keys: timestamp, difficulty, totalDifficulty, gasUsed, gasLimit

        difficulty : scalar value that represents how hard is was to validate the block
        totalDifficulty : summation of the difficulties of all previous blocks plus the current
        gasUsed: summation of gas fees in the block
        gasLimit: The limit of gas that can be included in a block

    Parameter:
        block_number (integer)

    Returns:
        data (dictionary)
        keys: timestamp, difficulty, totalDifficulty, gasUsed, gasLimit

    '''
    alc_key = os.environ.get('alc_key')
    url = f"https://eth-mainnet.g.alchemy.com/v2/{alc_key}"

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [str(hex(block_number)), True]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    data = {}
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    while(True):
        try:
            response = session.post(url, json=payload, headers=headers)
            json_response = json.loads(response.text)
            result = json_response['result']
            break
        except ValueError:
            pass

    
    unix_time = result['timestamp']
    hex_difficulty = result['difficulty']
    hex_total_difficulty = result['totalDifficulty']
    hex_gas_used = result['gasUsed']
    hex_gas_limit = result['gasLimit']

    unix_time = int(unix_time,16)
    timestamp = datetime.datetime.utcfromtimestamp(unix_time)

    data['timestamp'] = timestamp
    data['difficulty'] = int(hex_difficulty,16)
    data['total_difficulty'] = int(hex_total_difficulty,16)
    data['gas_used'] = int(hex_gas_used,16)
    data['gas_limit'] = int(hex_gas_limit,16)
    
    return data


def get_blocknumber(timestamp, before=True):
    '''
    Description:
        returns the block_number given the block confirmation timestamp

    Parameter:
        timestamp (timestamp)

    Returns:
        block_number (integer)
    '''
    timestamp = timestamp.replace('T', ' ')
    etherscan_key = os.environ.get('etherscan_key')
    url = 'https://api.etherscan.io/api'
    params = {    
    'module' : 'block',
    'action' : 'getblocknobytime',
    'closest' : 'before',
    'apikey' : etherscan_key
    }

    if before:
        params['closest'] = 'before'
    else:
        params['closest'] = 'after'

    timestamp = datetime.datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
    unix_time = time.mktime(timestamp.timetuple()) - (60*60*4) 
    params['timestamp'] = str(int(unix_time))
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    response = session.get(url,params=params)
    json_response = json.loads(response.text)

    return json_response['result']

def get_erc20_txns(address,block_number=None,timestamp=None,dump=False):
    '''
    Description:
        takes in an address and a block number or confirmation timestamp,
        and will return a list of transaction dictionaries.
        If dump=True, JSON response will be dumped to file

    Parameters:
        address          (string)
        KW: block_number (integer)    defaults to None
        KW: timestamp    (timestamp)  defaults to None
        dump             (Boolean)    defaults to False

    Returns:
        data (A list of dictionaries)

    Note:
        block_number or timestamp can be given, only one is required
    '''
    url = 'https://api.etherscan.io/api'
    etherscan_key = os.environ.get('etherscan_key')
    params = {
        'module':'account',
        'action':'tokentx',
        'apikey' : etherscan_key
    }
    if timestamp:
        timestamp = timestamp.replace('T', ' ')
        block_number = get_blocknumber(timestamp)
    params['startblock'] = block_number
    params['endblock'] = block_number
    params['address'] = address
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url,params=params)
    json_response = json.loads(response.text)
    if(dump):
        with open('txs.json','w') as f:
            json.dump(json_response,f,indent=4)

    data = json_response['result']
    return data

def get_int_txns(txn_hash, dump=False):
    '''
    Description:
        takes in a txn hash and returns with list of interal transaction
        dictionaries with transaction details
        If dump=True, JSON response will be dumped to file

    Parameters:
        txn_hash  (string)          
        dump      (Boolean)    defaults to False

    Returns
        data (list of dictionaries) 
    '''
    etherscan_key = os.environ.get('etherscan_key')
    url = 'https://api.etherscan.io/api'
    params = {
    'module' : 'account',
    'action' : 'txlistinternal',
    'apikey' : etherscan_key
    }
    params['txhash'] = txn_hash
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url,params=params)
    json_response = json.loads(response.text)
    if(dump):
        with open('int_txns.json','w') as f:
            json.dump(json_response,f,indent=4)
    
    data = json_response['result']
    return data

def get_normal_txn(address, block_number=None, timestamp=None, dump=False):
    '''
    Description:
        takes in an address and block_number and returns a list of
        dictionary containing normal transaction details
        If dump=True, JSON response will be dumped to file

    Parameters:
        address      (string)   
        block_number (integer)       
        dump         (Boolean)    defaults to False

    Returns
        data (list of dictionaries)
    
    '''
    etherscan_key = os.environ.get('etherscan_key')
    url = 'https://api.etherscan.io/api'
    params = {
    'module' : 'account',
    'action' : 'txlist',
    'apikey' : etherscan_key
    }
   
    if timestamp:
        timestamp = timestamp.replace('T', ' ')
        block_number = get_blocknumber(timestamp)
    params['startblock'] = block_number
    params['endblock'] = block_number
    params['address'] = address

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url,params=params)
    json_response = json.loads(response.text)
    if(dump):
        with open('normal_txs.json','w') as f:
            json.dump(json_response,f,indent=4)
    try:
        data = json_response['result']
    except IndexError:
        data = []
    return data


def get_price(address,timestamp):
    '''
    Description:
        takes in an address and timestamp and returns the
        sales price of one NFT in this transaction

    Parameters:
        address      (string)   
        block_number (integer)       
        dump         (Boolean)    defaults to False

    Returns
        sale_price (integer)
    
    '''
    etherscan_key = os.environ.get('etherscan_key')
    url = 'https://api.etherscan.io/api'
    params = {
        'module':'account',
        'action':'tokentx',
        'page' : '1',
        'offset' : '100',
        'startblock' : '0',
        'endblock' : '27025780',
        'sort' : 'asc',
        'apikey' : etherscan_key
    }

    blk_number = get_blocknumber(timestamp)
    params['startblock'] = blk_number
    params['endblock'] = blk_number
    params['address'] = address

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url,params=params)
    json_response = json.loads(response.text)
    transactions = json_response['result']
    sale_price = 0
    for transaction in transactions:
        sale_price += int(transaction['value'])


    return sale_price

def get_id(nft_event):
    '''
    Description:
        takes an NFT event and extracts the token_id

    Parameters:
        nft_event (JSON object)

    Returns
        token_id
    
    '''
    permalink = nft_event['payload']['payload']['item']['permalink']
    token_id = ''    
    for i in range(6):
        if(permalink[-i] == '/'):
            break
        token_id += permalink[-i]
    token_id = token_id[::-1][:-1]      # reverse and truncate last char
    return token_id


def insert_sql(nft_id,market_id,date_id,block_number,buyer,seller,
    txn_hash, sale_price):
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

    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    update_query = f'''
    INSERT INTO f_transaction
    (nft_id,market_id,date_id,block_id,buyer,seller,txn_hash, sale_price, updated_at)
    VALUES
    ({nft_id},{market_id},{date_id},{block_number},
    '{buyer}',
    '{seller}',
    '{txn_hash}',
    {sale_price},'{now}');
    '''

    return update_query


def get_logs(address, block_number):
    '''
    Description:
        returns the logs
        If dump=True, JSON response will be dumped to file

    Parameters:
        address      (string)    
        block_number (integer)      
        dump         (Boolean)    defaults to False

    Returns:
        logs (list of dictionaries) 
    '''
    etherscan_key = os.environ.get('etherscan_key')
    url = 'https://api.etherscan.io/api'
    params = {
    'module' : 'logs',
    'action' : 'getLogs',
    'page' : '1',
    'offset' : '100',
    'apikey' : etherscan_key
    }

    params['fromBlock'] = block_number
    params['toBlock'] = block_number
    params['address'] = address


    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    response = session.get(url,params=params)

    
    json_response = json.loads(response.text)
    with open('tx_logs.json','w') as f:
        json.dump(json_response,f,indent=4)
    logs = json_response['result']

    return logs


def get_checksum_address(address):
    '''
    Description:
        inputs lowercase ethereum address
        and returns checksum address

    Parameters:
        address (string)

    Returns: 
        checksum_address (string)
    '''
    address = address.replace('0x', '')
    keccak_hash = keccak.new(digest_bits=256)
    byte_address = str.encode(address)
    keccak_hash.update(byte_address)
    output_hash = keccak_hash.hexdigest()
    address_list = [*address]
    checksum_address = ''
    for i in range(0, len(address)):
        if(int(output_hash[i],16) <= 7):
            checksum_address += address[i]
        else:
            checksum_address += address[i].upper()

    checksum_address = f'0x{checksum_address}'
    return checksum_address

def sort_events(events):
    '''
    Description:
        Takes in a list of transaction events and sorts them by 
        the receiving address then by token_id

    Parameters:
        events (list of events)

    Returns:
        sorted_events (list of events)
    '''
    for event in events:
        if(event['event'] == 'item_transferred'):
            event['payload']['payload']['to_account']['address'] = (
                get_checksum_address(event['payload']['payload']['to_account']['address'])
            )
        else:
            event['payload']['payload']['to_account'] = {}
            event['payload']['payload']['to_account']['address'] = (
                get_checksum_address(event['payload']['payload']['taker']['address'])
            )

        event['payload']['payload']['item']['token_id'] = (
            get_id(event)
        )

    events.sort(key=lambda x: (x['payload']['payload']['to_account']['address'],
    x['payload']['payload']['item']['token_id']))

    return events







