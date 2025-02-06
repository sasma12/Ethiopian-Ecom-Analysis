from telethon import TelegramClient
import pandas as pd

api_id = '28089363'
api_hash = 'a5880c56971db2fb479e1e254b24bd9e'
channels = ['@helloomarketethiopia', '@easybuyethiopia', '@EOMMarket']

client = TelegramClient('session_name', api_id, api_hash)

async def scrape_messages():
    await client.start()
    all_messages = []
    
    for channel in channels:
        messages = await client.get_messages(channel, limit=100)
        for message in messages:
            all_messages.append({
                'sender_id': message.sender_id,
                'text': message.text,
                'timestamp': message.date
            })
    return all_messages

with client:
    messages = client.loop.run_until_complete(scrape_messages())

# Save the data to CSV or a DataFrame
df = pd.DataFrame(messages)
df.to_csv('data/raw_data/telegram_data.csv', index=False)  # save the data
print("Data scraped and saved successfully!")
