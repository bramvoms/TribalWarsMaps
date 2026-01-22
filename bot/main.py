import discord
from discord.ext import commands
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
import logging
import asyncpg
from config import default_intents

# Logging
logging.getLogger("discord").setLevel(logging.WARNING)

# Env
load_dotenv()

intents = default_intents()

bot = commands.Bot(
    command_prefix="*",
    intents=intents,
    application_id=int(os.getenv("DISCORD_APPLICATION_ID")),
    reconnect=True
)

# ------------------------------------------------------------------
# EMBED HELPER
# ------------------------------------------------------------------

EMBED_COLOR = discord.Color.from_rgb(221, 205, 165)

def create_embed(title: str = None, description: str = None) -> discord.Embed:
    embed = discord.Embed(description=description, color=EMBED_COLOR)
    if title:
        embed.title = title
    return embed

# ------------------------------------------------------------------

@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
        print("Command tree synced.")
    except Exception as e:
        print(f"Slash sync error: {e}")

async def load_cogs():
    cogs_path = Path(__file__).parent / "cogs"
    for file in cogs_path.glob("*.py"):
        cog = f"cogs.{file.stem}"
        try:
            await bot.load_extension(cog)
            print(f"Loaded cog: {cog}")
        except Exception as e:
            print(f"Failed loading {cog}: {e}")

async def main():
    print("Starting bot")

    bot.db = await asyncpg.create_pool(os.getenv("DATABASE_URL"))

    cogs_dir = Path(__file__).parent / "cogs"
    cogs_dir.mkdir(exist_ok=True)

    await load_cogs()
    await bot.start(os.getenv("DISCORD_TOKEN"))

if __name__ == "__main__":
    asyncio.run(main())
