import asyncio
import datetime
from datetime import timedelta
import os
import io
import pandas as pd
import boto3
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

channels = [
    "combatnews7723",
    "thiha11223344",
    "factcheckmm",
    "pdfnugterrorist2021",
    "westernnews24",
    "dgf21news",
    "NPNewsMyanmar",
    "MyanmarNationalPost",
    "peoplemedianews",
    "aw864",
    "naysinthadin",
    "peoplemedianews",
    "threebrotherhoodalliance"
]

api_id = '9324313'
api_hash = 'e5f895ec6fa7c608a62e722a28580f26'
client = TelegramClient('Test', api_id, api_hash)
client.start()

async def process_channel(channel, dates):
    for date in dates:
        try:
            channel_entity = await client.get_entity(channel)
            start_date = datetime.datetime.strptime(date, '%Y-%m-%d')
            end_date = start_date + datetime.timedelta(days=1)
            prefirst_m = await client.get_messages(channel_entity, limit=1, offset_date=start_date)
            first_m = await client.get_messages(channel_entity, min_id=prefirst_m[0].id, limit=1, reverse=True)
            last_m = await client.get_messages(channel_entity, limit=1, offset_date=end_date)
            messages_between = await client.get_messages(channel_entity, min_id=first_m[0].id, max_id=last_m[0].id)
            
            s3_client = boto3.client('s3')
            bucket_name = "telegramdb"

            if messages_between:
                messages_between.insert(0, last_m[0])
                messages_between.append(first_m[0])
            
            if last_m[0].id == first_m[0].id:
                messages_between = last_m

            data = []
            channel_folder = os.path.join(channel_entity.username, date, "media")

            for m in messages_between:
                media_info = "None"
                if isinstance(m.media, MessageMediaPhoto):
                    filename = f'{channel_entity.username}photo{m.id}.jpg'
                    filepath = os.path.join(channel_folder, filename)
                    photo_data = await client.download_file(m.media)
                    photo_stream = io.BytesIO(photo_data)
                    s3_client.upload_fileobj(photo_stream, bucket_name, filepath)
                    s3_url = f'https://{bucket_name}.s3.amazonaws.com/{filepath}'
                    media_info = s3_url
                    print(media_info)

                if isinstance(m.media, MessageMediaDocument):
                    filename = f'{channel_entity.username}video{m.id}.avi'
                    filepath = os.path.join(channel_folder, filename)
                    if m.file.size <= 10000000:
                        photo_data = await client.download_file(m.media)
                        photo_stream = io.BytesIO(photo_data)
                        s3_client.upload_fileobj(photo_stream, bucket_name, filepath)
                        s3_url = f'https://{bucket_name}.s3.amazonaws.com/{filepath}'
                        media_info = s3_url

                id = str(m.id).replace(',', '')
                link = f"https://t.me/{channel_entity.username}/{id}"
                data.append({'channel': channel_entity.username, 'text': m.text, 'id': id, 'date': m.date, 'extraction_date': datetime.datetime.now(), 'link': link, 'reactions': m.reactions, 'views': m.views, 'media_info': media_info})
            
            df = pd.DataFrame(data)
            df['date'] = df['date'].dt.tz_localize(None)
            end_date = end_date - datetime.timedelta(days=1)
            
            filename = f'{channel_entity.username}_{start_date.date()}.xlsx'

            if messages_between:
                file_directory = os.path.join("Files", channel_entity.username)
                os.makedirs(file_directory, exist_ok=True)
                file_path = os.path.join(file_directory, filename)
                df.to_excel(file_path, index=False)
                path = f"{channel_entity.username}/{date}/" + filename
                s3_client.upload_file(file_path, bucket_name, path)

            print(f"Data saved for {channel_entity.username} on {date}")
            
        except Exception as e:
            print(f"An error occurred for {channel}: {e}")

async def main():
    s_date = input("Enter start date (YYYY-MM-DD): ")
    e_date = input("Enter end date (YYYY-MM-DD): ")
    dates = []

    s_date = datetime.datetime.strptime(s_date, '%Y-%m-%d')
    e_date = datetime.datetime.strptime(e_date, '%Y-%m-%d')

    current_date = s_date
    while current_date <= e_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    for channel in channels:
        await process_channel(channel, dates)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
