import discord
import aiohttp
import asyncpg
import asyncio
from discord.ext import commands, tasks
from discord import app_commands
from typing import List
from datetime import datetime, timedelta
import re
import asyncio
import pytz


class ConquerTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_conquers.start()

    async def create_tables(self):
        """Ensure required tables exist."""
        await self.bot.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_settings_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                tribe_id BIGINT NOT NULL,
                starting_unix_timestamp BIGINT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world, tribe_id)
            );
        """)

        await self.bot.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_data_v2 (
                world TEXT NOT NULL,
                village_id BIGINT NOT NULL,
                unix_timestamp BIGINT NOT NULL,
                new_owner_id BIGINT NOT NULL,
                new_owner_tribe_id BIGINT,
                old_owner_id BIGINT NOT NULL,
                old_owner_tribe_id BIGINT,
                points INT NOT NULL,
                PRIMARY KEY (world, village_id, unix_timestamp, new_owner_id, old_owner_id)
            );
        """)

        await self.bot.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_messages_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                village_id BIGINT NOT NULL,
                unix_timestamp BIGINT NOT NULL,
                old_owner_id BIGINT NOT NULL,
                new_owner_id BIGINT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world, village_id, unix_timestamp, old_owner_id, new_owner_id)
            );
        """)

    async def cog_load(self):
        """Asynchronous hook to ensure tables are created when the cog is loaded."""
        await self.create_tables()
        if not self.check_conquers.is_running():
            self.check_conquers.start()
        
    async def cog_unload(self):
        self.check_conquers.cancel() 
        
    async def get_tribe_id(self, world, tribe_tag):
        """Get the tribe ID and exact tag from ally_data for the given world."""
        query = """
            SELECT tribe_id, tag FROM ally_data
            WHERE world = $1 AND tag = $2
        """
        return await self.bot.db.fetchrow(query, world, tribe_tag)

    async def toggle_tracking(self, guild_id, channel_id, world, tribe_tag):
        """Enable or disable conquer tracking for a specific tribe in a world."""
        tribe_data = await self.get_tribe_id(world, tribe_tag)
        channel = self.bot.get_channel(channel_id)

        if not tribe_data:
            if channel:
                await channel.send(
                    f"Stammen tag `{tribe_tag}` niet gevonden op wereld `{world}`. "
                    f"Gebruik de exacte tag zoals ingame."
                )
            return None

        tribe_id, exact_tag = tribe_data 

        exists_query = """
            SELECT 1 FROM conquer_settings_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4
        """
        exists = await self.bot.db.fetchval(
            exists_query, guild_id, channel_id, world, tribe_id
        )

        if exists:
            delete_query = """
                DELETE FROM conquer_settings_v2
                WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4
            """
            await self.bot.db.execute(
                delete_query, guild_id, channel_id, world, tribe_id
            )
            if channel:
                await channel.send(
                    f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` uitgeschakeld."
                )
            
            if not await self.bot.db.fetchval("SELECT 1 FROM conquer_settings_v2"):
                if self.check_conquers.is_running():
                    self.check_conquers.cancel()
            
            return False
        else:
            insert_query = """
                INSERT INTO conquer_settings_v2 (
                    guild_id, channel_id, world, tribe_id, starting_unix_timestamp
                )
                VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
            """
            await self.bot.db.execute(
                insert_query, guild_id, channel_id, world, tribe_id
            )
            if channel:
                await channel.send(
                    f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` aangezet."
                )

            if not self.check_conquers.is_running():
                self.check_conquers.start()
            
            return True

    @tasks.loop(minutes=1)
    async def check_conquers(self):
        """Fetch and process conquers for all tracked worlds."""
        if not self.bot.is_ready():
            return
    
        tracking_data = await self.bot.db.fetch("SELECT * FROM conquer_settings_v2")
        if not tracking_data:
            print("[ConquerTracker] - Geen ingeschakelde stammen gevonden. Loop draait wel.")
            return
    
        world_conquer_counts = {entry["world"]: 0 for entry in tracking_data}
    
        for entry in tracking_data:
            guild_id = entry["guild_id"]
            channel_id = entry["channel_id"]
            world = entry["world"]
            tracked_tribe_id = entry["tribe_id"]
            starting_timestamp = entry["starting_unix_timestamp"]
            timestamp_23_hours_ago = int(datetime.utcnow().timestamp()) - 82800
            url = (
                f"https://{world}.tribalwars.nl/interface.php"
                f"?func=get_conquer_extended&since={timestamp_23_hours_ago}"
            )
    
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[DEBUG] Failed to fetch conquer data from {url}, status code: {response.status}")
                        continue
                    raw_data = await response.text()
                    conquers = [e.split(',') for e in re.split(r'\s+', raw_data.strip()) if e]
    
            for conquer in conquers:
                if len(conquer) < 4:
                    print(f"[DEBUG] Skipping invalid conquer data: {conquer}")
                    continue
    
                village_id, unix_timestamp, new_owner_id, old_owner_id = map(int, conquer[:4])
    
                stored = await self.store_conquer(world, village_id, unix_timestamp, new_owner_id, old_owner_id)
                if stored:
                    world_conquer_counts[world] += 1
    
                player_query = """
                    SELECT player_id, tribe_id FROM player_data
                    WHERE world = $1 AND player_id IN ($2, $3)
                """
                players = await self.bot.db.fetch(player_query, world, new_owner_id, old_owner_id)
                
                new_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == new_owner_id), None)
                old_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == old_owner_id), None)
        
                tracking_query = """
                    SELECT guild_id, channel_id, tribe_id
                    FROM conquer_settings_v2
                    WHERE world = $1
                """
                tracking_channels = await self.bot.db.fetch(tracking_query, world)
    
                if not tracking_channels:
                    continue
    
                relevant_channels = [
                    t for t in tracking_channels
                    if t["tribe_id"] in (new_owner_tribe_id, old_owner_tribe_id)
                ]
      
                if not relevant_channels:
                    continue 
    
                for tracking in relevant_channels:
                    rguild_id = tracking["guild_id"]
                    rchannel_id = tracking["channel_id"]
                    rtracked_tribe_id = tracking["tribe_id"]
                    await self.process_conquer(
                        rguild_id,
                        rchannel_id,
                        world,
                        rtracked_tribe_id,
                        village_id,
                        unix_timestamp,
                        new_owner_id,
                        old_owner_id
                    )
    
        for world, count in world_conquer_counts.items():
            if count == 0:
                print(f"[ConquerTracker {world.upper()}] - 0 new conquers found.")
            else:
                print(f"[ConquerTracker {world.upper()}] - {count} new conquers found.")

    async def store_conquer(self, world, village_id, unix_timestamp, new_owner_id, old_owner_id):
        """Store conquer in database if not already recorded."""
        exists_query = """
            SELECT 1 FROM conquer_data_v2
            WHERE world = $1 AND village_id = $2 AND unix_timestamp = $3
              AND new_owner_id = $4 AND old_owner_id = $5
        """
        exists = await self.bot.db.fetchval(
            exists_query, world, village_id, unix_timestamp, new_owner_id, old_owner_id
        )
        if exists:
            return False
    
        player_query = """
            SELECT player_id, tribe_id FROM player_data
            WHERE world = $1 AND player_id IN ($2, $3)
        """
        players = await self.bot.db.fetch(player_query, world, new_owner_id, old_owner_id)
    
        village_query = """
            SELECT points FROM village_data
            WHERE world = $1 AND village_id = $2
        """
        village = await self.bot.db.fetchrow(village_query, world, village_id)
    
        if not village:
            return False
    
        new_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == new_owner_id), None)
        old_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == old_owner_id), None)
        points = village["points"]
    
        insert_query = """
            INSERT INTO conquer_data_v2 (
                world, village_id, unix_timestamp,
                new_owner_id, new_owner_tribe_id,
                old_owner_id, old_owner_tribe_id,
                points
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        await self.bot.db.execute(
            insert_query,
            world,
            village_id,
            unix_timestamp,
            new_owner_id,
            new_owner_tribe_id,
            old_owner_id,
            old_owner_tribe_id,
            points
        )
        
        return True

    async def process_conquer(
        self,
        guild_id,
        channel_id,
        world,
        tracked_tribe_id,
        village_id,
        unix_timestamp,
        new_owner_id,
        old_owner_id
    ):
        """Send conquer messages to all channels where the tribe is enabled and store them in conquer_messages_v2."""
        conquer_query = """
            SELECT new_owner_tribe_id, old_owner_tribe_id FROM conquer_data_v2
            WHERE world = $1 AND village_id = $2 AND unix_timestamp = $3
              AND new_owner_id = $4 AND old_owner_id = $5
        """
        conquer = await self.bot.db.fetchrow(
            conquer_query,
            world,
            village_id,
            unix_timestamp,
            new_owner_id,
            old_owner_id
        )
        
        if not conquer:
            return 
    
        new_owner_tribe_id = conquer["new_owner_tribe_id"]
        old_owner_tribe_id = conquer["old_owner_tribe_id"]
    
        tribe_query = """
            SELECT tribe_id, tag FROM ally_data
            WHERE world = $1 AND tribe_id IN ($2, $3)
        """
        tribes = await self.bot.db.fetch(
            tribe_query,
            world,
            new_owner_tribe_id,
            old_owner_tribe_id
        )
    
        new_owner_tribe_tag = next(
            (t["tag"] for t in tribes if t["tribe_id"] == new_owner_tribe_id),
            None
        )
        old_owner_tribe_tag = next(
            (t["tag"] for t in tribes if t["tribe_id"] == old_owner_tribe_id),
            None
        )
    
        player_query = """
            SELECT player_id, name FROM player_data
            WHERE world = $1 AND player_id IN ($2, $3)
        """
        players = await self.bot.db.fetch(
            player_query,
            world,
            new_owner_id,
            old_owner_id
        )
    
        village_query = """
            SELECT name, x, y, points FROM village_data
            WHERE world = $1 AND village_id = $2
        """
        village = await self.bot.db.fetchrow(village_query, world, village_id)
    
        if not village:
            return 
    
        village_name = village["name"]
        x = village["x"]
        y = village["y"]
        points = village["points"]

        new_owner = next((p for p in players if p["player_id"] == new_owner_id), None)
        old_owner = next((p for p in players if p["player_id"] == old_owner_id), None)
    
        new_owner_name = new_owner["name"] if new_owner else "Onbekend"
        old_owner_name = old_owner["name"] if old_owner else "Barbarendorp"
        
        exists_query = """
            SELECT 1 FROM conquer_messages_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3
              AND village_id = $4 AND unix_timestamp = $5
              AND old_owner_id = $6 AND new_owner_id = $7
        """
        exists = await self.bot.db.fetchval(
            exists_query,
            guild_id,
            channel_id,
            world,
            village_id,
            unix_timestamp,
            old_owner_id,
            new_owner_id
        )
        
        if exists:
            return  
            
        color = discord.Color.default()
        description = ""

        if new_owner_tribe_id == tracked_tribe_id and old_owner_id == 0:
            description = f"**{new_owner_name}** heeft een barbarendorp veroverd!"
            color = discord.Color.green()
        elif (
            new_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id == old_owner_tribe_id
            and new_owner_id != old_owner_id
        ):
            description = (
                f"**{new_owner_name}** heeft een dorp veroverd van zijn/haar stamgenoot "
                f"**{old_owner_name}**!"
            )
            color = discord.Color.yellow()
        elif new_owner_tribe_id == tracked_tribe_id and new_owner_id == old_owner_id:
            description = f"**{new_owner_name}** heeft zichzelf veroverd!"
            color = discord.Color.yellow()
        elif (
            old_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id != old_owner_tribe_id
            and new_owner_tribe_id == 0
        ):
            description = f"**{old_owner_name}** is een dorp verloren aan **{new_owner_name}**!"
            color = discord.Color.red()
        elif (
            old_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id != old_owner_tribe_id
            and new_owner_tribe_id != 0
        ):
            description = (
                f"**{old_owner_name}** is een dorp verloren aan **{new_owner_name}** "
                f"(`{new_owner_tribe_tag}`)!"
            )
            color = discord.Color.red()
        elif new_owner_tribe_id == tracked_tribe_id and old_owner_tribe_id == 0:
            description = (
                f"**{new_owner_name}** heeft een dorp veroverd van **{old_owner_name}**!"
            )
            color = discord.Color.green()
        elif new_owner_tribe_id == tracked_tribe_id and old_owner_tribe_id != 0:
            description = (
                f"**{new_owner_name}** heeft een dorp veroverd van **{old_owner_name}** "
                f"(`{old_owner_tribe_tag}`)!"
            )
            color = discord.Color.green()
            
        timezone = pytz.timezone("Europe/Amsterdam")
        local_dt = datetime.utcfromtimestamp(unix_timestamp).replace(
            tzinfo=pytz.utc
        ).astimezone(timezone)
        local_time = local_dt.strftime("%Y-%m-%d %H:%M:%S")
    
        embed = discord.Embed(description=description, color=color)
        embed.add_field(
            name="Dorp",
            value=f"```{village_name} ({x}|{y})```",
            inline=True
        )
        embed.add_field(
            name="Punten",
            value=f"```{str(points)}```",
            inline=True
        )
        embed.set_footer(text=f"Tijdstip: {local_time}")
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            print(f"[DEBUG] Channel {channel_id} not found, message not sent.")
        else:
            try:
                await channel.send(embed=embed)
                await asyncio.sleep(1)  # ✅ Kleine delay tegen rate limits
            except discord.Forbidden:
                print(f"[DEBUG] Geen toegang tot kanaal {channel_id} in guild {guild_id} (Forbidden). Bericht niet verstuurd.")
                return
            except discord.HTTPException as e:
                print(f"[DEBUG] HTTPException bij sturen naar kanaal {channel_id}: {e}")
                return
            
        insert_query = """
            INSERT INTO conquer_messages_v2 (
                guild_id, channel_id, world, village_id,
                unix_timestamp, old_owner_id, new_owner_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
        """
        await self.bot.db.execute(
            insert_query,
            guild_id,
            channel_id,
            world,
            village_id,
            unix_timestamp,
            old_owner_id,
            new_owner_id
        )

async def setup(bot):
    cog = ConquerTracker(bot)
    await cog.create_tables()  # Ensures table is created before the task starts
    await bot.add_cog(cog)
