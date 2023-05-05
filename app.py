from telethon import TelegramClient
from telethon.tl.types import InputPeerUser
from telethon.sync import TelegramClient, events
import asyncio
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import pyarrow as pa
import json
import pandas as pd
import os
import datetime
import argparse
from telethon import utils
from io import BytesIO
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

api_id = 27594652
#obtain current path
tmp_path  = os.getcwd()
blob_service_client = BlobServiceClient.from_connection_string(os.environ.get('CONNECTION_STRING'))
container_client = blob_service_client.get_container_client(os.environ.get('CONTAINER_NAME'))
#BOT_TOKEN = '6114626207:AAEjGJdX2j-H73-9OJbqFR1_4UzFGUnu-yY'
current_messages = []


def saveMessageTorParquet(message, file_name):
    global current_messages
    current_messages.append(message)
    if len(current_messages) >= os.environ.get('EACH_MANY'):
        df = pd.DataFrame(current_messages)
        table = pa.Table.from_pandas(df)
        current_messages.clear()
        with container_client.get_blob_client(tmp_path+ file_name) as blob_client:
            with BytesIO() as f:
                pq.write_table(table, f)
                f.seek(0)
                print("Saving to azure "+str(os.environ.get('EACH_MANY'))+" registers")
                blob_client.upload_blob(f, overwrite=True)

def on_message(message):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f"{timestamp+'_'+os.environ.get('CONTAINER_NAME')}.parquet"
    saveMessageTorParquet(message, file_name)
    


async def main():
    client = TelegramClient('session_read', os.environ.get('API_ID'), os.environ.get('API_HASH'))
    
    input_sender = InputPeerUser(os.environ.get('CASTILLO_EMITTER'), os.environ.get('CASTILLO_EMITTER'))
    @client.on(events.NewMessage(from_users=[input_sender]))
    async def receipt_message(event):
        message_date = event.date.strftime("%Y-%m-%d %H:%M:%S") # formatea la fecha
        message_content = event.message.message # obtiene el contenido del mensaje
        #print(f"[{message_date}] {message_content}")
        on_message({'timestamp':message_date,'data':message_content})

    await client.start()
    await client.run_until_disconnected()

# Llama a la función main utilizando asyncio.run()
asyncio.run(main())