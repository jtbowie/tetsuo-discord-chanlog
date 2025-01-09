from logging import NullHandler
import discord
import sqlite3
import os
from discord.ext import tasks, commands
from discord.ext.commands import check
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
STEALTH = True

intents = discord.Intents.default()
intents.message_content = True  # Required for reading message content
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# Initialize SQLite database
try:
    db = sqlite3.connect('discord_messages.db')
    cursor = db.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            channel_id TEXT,
            author_id TEXT,
            content TEXT,
            sentiment FLOAT,
            timestamp TEXT,
        )
        """
    )
    db.commit()
except sqlite3.Error as e:
    print(f"Database connection failed: {e}... Exiting")
    exit(1)

# Function to execute SQL commands (synchronous)
def execute_sql(sql, params=None):
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        db.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        print(f"SQL: {sql}")

# Check for roles
def has_roles(*role_names):
    async def predicate(ctx):
        author_roles = set(ctx.author.roles)
        print(f"author roles: {author_roles}")
        required_roles = [discord.utils.get(ctx.guild.roles, name=name) for name in role_names]
        return any(role in author_roles for role in required_roles if role)
    return check(predicate)

# Bot events
@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")

# Command definition
@bot.command(name='log')
@has_roles('owner', 'tetsuo core dev')
async def log(ctx):
    """Says hello to the user who invoked the command."""
    if not STEALTH:
        await ctx.send(f'Logging messages from channel {ctx.channel.name}...')
    await fetch_initial_messages(ctx)

async def fetch_initial_messages(ctx):
    channel = ctx.channel
    if channel:
        # Use an async list comprehension to handle the async generator
        messages = [msg async for msg in channel.history(limit=250)]
        
        for message in messages:
            if message.content.startswith("Sentiment"):
                compound = float(message.content.split(" ")[-1][:-1])
                execute_sql(
                    """
                    INSERT OR REPLACE INTO messages (message_id, channel_id, author_id, content, timestamp, sentiment) 
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(message.id),
                        str(channel.id),
                        str(message.author.id),
                        message.content,
                        message.created_at.isoformat(),
                        compound
                    )
                )
            else:
                execute_sql(
                    """
                    INSERT OR REPLACE INTO messages (message_id, channel_id, author_id, content, timestamp) 
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(message.id),
                        str(channel.id),
                        str(message.author.id),
                        message.content,
                        message.created_at.isoformat(),
                    )
                )


        print(f"Fetched and stored {len(messages)} messages.")
    else:
        print("Channel not found.")

monitored_channels = []


@bot.event
async def on_message(message):
    if not message.content.startswith("!"):
        if message.author.bot or message.channel not in monitored_channels:
            return
    else:
        await bot.process_commands(message)

    attachment_blob = None
    attachment_name = None
    if message.attachments:
        attachment = message.attachments[0]
        attachment_blob = await attachment.read()  # Now async
        attachment_name = attachment.filename

    execute_sql(
        """
        INSERT OR REPLACE INTO messages (message_id, channel_id, author_id, content, timestamp, attachment, attachment_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(message.id),
            str(message.channel.id),
            str(message.author.id),
            message.content,
            message.created_at.isoformat(),
            attachment_blob if attachment_blob else None,
            attachment_name,
        )
    )
    print(f"New message logged: {message.content}")

def main():
    if not DISCORD_TOKEN:
        print("Must supply DISCORD_TOKEN environment variable")
        exit(1)
    
    bot.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
