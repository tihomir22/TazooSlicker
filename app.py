from telethon import TelegramClient
from telethon.tl.types import InputPeerUser
from telethon.sync import TelegramClient, events
import asyncio
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import os
import datetime
from telethon import utils
from io import BytesIO
from dotenv import load_dotenv
import uuid

load_dotenv()  
blob_service_client = BlobServiceClient.from_connection_string(os.environ.get('CONNECTION_STRING'))
container_client = blob_service_client.get_container_client(os.environ.get('CONTAINER_NAME'))
current_messages = []


def saveMessageTorParquet(message, file_name):
    global current_messages
    current_messages.append(message)
    if len(current_messages) >= int(os.environ.get('EACH_MANY')):
        df = pd.DataFrame(current_messages)
        table = pa.Table.from_pandas(df)
        current_messages.clear()
        with container_client.get_blob_client(file_name) as blob_client:
            with BytesIO() as f:
                pq.write_table(table, f)
                f.seek(0)
                print("Saving to azure "+os.environ.get('EACH_MANY')+" registers")
                blob_client.upload_blob(f, overwrite=True)

def on_message(message):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f"{timestamp+'_'+os.environ.get('CONTAINER_NAME')+'_'+str(uuid.uuid4())}.parquet"
    saveMessageTorParquet(message, file_name)
    


async def main():
    client = TelegramClient('session_read', int(os.environ.get('API_ID')), os.environ.get('API_HASH'))
    
    input_sender = InputPeerUser(int(os.environ.get('CASTILLO_EMITTER')), int(os.environ.get('CASTILLO_EMITTER')))
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