import asyncio
import websockets
import ssl
import certifi
import json
import time
import logging
import os
from datetime import datetime as dt

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())


logger = logging.getLogger('websockets')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


# JSON message I need to send to API to establish subscription to the publiser
heartbeat = {
  "topic": "phoenix",
  "event": "heartbeat",
  "payload": {},
  "ref": 0
}

# dictionary for converting Collection slug to Collection symbol
symbols = {
    "boredapeyachtclub" : 'bayc',
    "mutant-ape-yacht-club" : 'mayc',
    "bored-ape-kennel-club" : 'bakc',
    "azuki" : "azuki"
}
#--------------------------------------------------------------------------
#---------------------------READ THIS-------------------------------------
# uncomment one of the lines below to select which collection to subscribe to   
#collection = "boredapeyachtclub"
#collection = "mutant-ape-yacht-club"
#collection = "bored-ape-kennel-club"
collection = "azuki"

symbol = symbols[collection]

slug = {
  "topic": f"collection:{collection}",
  "event": "phx_join",
  "payload": {},
  "ref": 0
}

# used for identifying batch data
now = dt.utcnow().strftime('%m_%d_%y_%H')
HOUR = dt.utcnow().strftime('%H')

# prefix is used for formulate the file-path of our data
prefix = f'../data/{symbol}/{symbol}_sales_data' 

# Grabbing the Websocket ID, Used for EC2 instance identification
WS_ID = os.getenv('WS_ID')
print(f"//////// {WS_ID} /////////")

file_name = f"{prefix}_{now}_{WS_ID}.json"
heartbeat = json.dumps(heartbeat)
slug = json.dumps(slug)
opensea_key = os.environ.get("opensea_key")
uri = f"wss://stream.openseabeta.com/socket/websocket?token={opensea_key}"


# Booleans for file output decisions
NEW_FILE = False
FIRST_EVENT = True
event_types = []
async def subscribe_and_wait():
    '''
    asynchronus function that subscribes to NFT collection event stream,
    and writes NFT sales and transfers to file
    '''
    global NEW_FILE
    global HOUR
    global FIRST_EVENT
    websocket = await websockets.connect(uri,ssl=ssl_context, ping_timeout=60000)
    await asyncio.sleep(2)
    await websocket.send(slug)
    msg = await websocket.recv()
    print(msg)
    f = open(fname,'w')
    # instantiate our JSON file
    f.write('{"data": [')
    # this code runs indefinetly 
    while True:
        try:
            # if not connected to the web socket ping reconnection message
            # and wait for a message to be received
            if not websocket.open:
                print('Websocket NOT connected. Trying to reconnect')
                websocket = await websockets.connect(uri,ssl=ssl_context,
                ping_timeout=60000)
                await websocket.send(slug)
                msg = await websocket.recv()
                print(msg)
            # wait for message
            msg = await websocket.recv()
            json_message = json.loads(msg)
            # once messaged is received, check to see if it is a new hour
            cur_hour = int(dt.utcnow().strftime('%H'))
            if(cur_hour != int(HOUR)):
                NEW_FILE = True
                FIRST_EVENT = True
            if(NEW_FILE):
                print('///NEW FILE///')
                f.write(']}')
                f.close()
                now = dt.utcnow().strftime('%m_%d_%y_%H')
                name = f"{prefix}_{now}_{WS_ID}.json"
                f = open(name, 'w')
                f.write('{"data": [')
                NEW_FILE = False
                HOUR = dt.utcnow().strftime('%H')
            # output event type received to validate websocket connection
            print(json_message['event'])
            # we only want to record known NFT sales or NFT transfers
            # NFT transfers could be sales from another platform
            if(json_message['event'] == 'item_transferred' 
                or json_message['event'] == 'item_sold'):
                payload = json_messagejson_message['payload']['payload']
                time_sent = payload['event_timestamp']
                cli_msg = f"{time_sent[11:19]} : {json_message['event']}"
                print(cli_msg)
                if(FIRST_EVENT):
                    json.dump(json_message,f,indent=4)
                    FIRST_EVENT = False
                    continue
                f.write(",")
                json.dump(json_message,f,indent=4)
        except:
            pass

async def ping():
    '''
    pings the web socket every 30 seconds to ensure open connection
    '''
    uri = f"wss://stream.openseabeta.com/socket/websocket?token={opensea_key}"
    websocket = await websockets.connect(uri,ssl=ssl_context, ping_timeout=60000)
    while True:
        try:
            if not websocket.open:
                print('Websocket NOT connected. Trying to reconnect')
                websocket = await websockets.connect(uri,ssl=ssl_context, ping_timeout=60000)
                await websocket.send(heartbeat)
                await asyncio.sleep(30)
            else:
                await websocket.send(heartbeat)
                await asyncio.sleep(30)
        except:
            pass




async def main():
    # Start both tasks concurrently
    await asyncio.gather(subscribe_and_wait(), ping())

if __name__ == '__main__':
    asyncio.run(main())

