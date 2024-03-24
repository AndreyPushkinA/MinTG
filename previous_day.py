import asyncio
from datetime import timedelta, datetime
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
    "MyanmarNationalPost",
    "peoplemedianews",
    "NPNewsMyanmar",
    "aw864",
    "naysinthadin",
    "peoplemedianews",
    "threebrotherhoodalliance"
]


# these are credentials for test Telegram API account, you should use it with Test.session file, 
# or create your own using https://my.telegram.org/auth
api_id = "9324313"
api_hash = "e5f895ec6fa7c608a62e722a28580f26"
client = TelegramClient("Test", api_id, api_hash)
client.start()


# binding messages belong to one post
def convert(data):
    df = pd.DataFrame(data)
    columns = [
        "channel",
        "text",
        "group_id",
        "date",
        "time",
        "extraction_date",
        "id",
        "link",
        "reactions",
        "media_info",
    ]
    df_selected = df[columns]

    def join_unique_values(x):
        if x.nunique() == 1:
            return x.iloc[0]
        else:
            return " ".join(x.astype(str).unique())

    combined_df = (
        df_selected.groupby(["channel", "group_id"])
        .agg(lambda x: "".join(join_unique_values(x.astype(str))))
        .reset_index()
    )

    return combined_df


# extract telegram posts and save it to s3 database
async def process_channel(channel, date):
    try:
        channel_entity = await client.get_entity(channel)
        start_date = datetime.strptime(date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        start_date -= timedelta(hours=6, minutes=30)
        end_date = start_date + timedelta(hours=24)
        prefirst_m = await client.get_messages(
            channel_entity, limit=1, offset_date=start_date
        )
        first_m = await client.get_messages(
            channel_entity, min_id=prefirst_m[0].id, limit=1, reverse=True
        )
        last_m = await client.get_messages(
            channel_entity, limit=1, offset_date=end_date
        )
        messages_between = await client.get_messages(
            channel_entity, min_id=first_m[0].id, max_id=last_m[0].id
        )

        s3_client = boto3.client("s3")
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
            group_id = str(m.id)
            if isinstance(m.media, MessageMediaPhoto):
                filename = f"{channel_entity.username}photo{m.id}.jpg"
                filepath = os.path.join(channel_folder, filename)
                photo_data = await client.download_file(m.media)
                photo_stream = io.BytesIO(photo_data)
                s3_client.upload_fileobj(photo_stream, bucket_name, filepath)
                s3_url = f"https://{bucket_name}.s3.amazonaws.com/{filepath}"
                media_info = s3_url

            if isinstance(m.media, MessageMediaDocument):
                filename = f"{channel_entity.username}video{m.id}.avi"
                filepath = os.path.join(channel_folder, filename)
                if m.file.size <= 1000000:
                    photo_data = await client.download_file(m.media)
                    photo_stream = io.BytesIO(photo_data)
                    s3_client.upload_fileobj(photo_stream, bucket_name, filepath)
                    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{filepath}"
                    media_info = s3_url
            if m.grouped_id:
                group_id = m.grouped_id
            else:
                group_id = m.id

            date_time = m.date 
            date_time += timedelta(hours=6, minutes=30)
            date = date_time.date()
            time = date_time.time()

            id = str(m.id).replace(",", "")

            link = f"https://t.me/{channel_entity.username}/{id}"

            data.append(
                {
                    "channel": channel_entity.username,
                    "text": m.text,
                    "id": id,
                    "date": date,
                    "time": time,
                    "extraction_date": date.today(),
                    "link": link,
                    "reactions": m.reactions,
                    "group_id": group_id,
                    "views": m.views,
                    "media_info": media_info,
                }
            )

        data = convert(data)

        df = pd.DataFrame(data)

        end_date = end_date - timedelta(days=1)

        filename = f"{channel_entity.username}_{date}.xlsx"

        file_directory = os.path.join("Files", channel_entity.username)
        os.makedirs(file_directory, exist_ok=True)
        file_path = os.path.join(file_directory, filename)
        df.to_excel(file_path, index=False)
        path = f"{channel_entity.username}/{date}/" + filename
        s3_client.upload_file(file_path, bucket_name, path)
        os.remove(file_path)

        print(f"Data saved for {channel_entity.username} on {date}")

    except Exception as e:
        print(f"An error occurred for {channel}: {e}")


async def main():
    s_date = str(datetime.today().date() - timedelta(1))

    for channel in channels:
        await process_channel(channel, s_date)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
