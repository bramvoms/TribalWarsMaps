import discord
import aiohttp
import asyncpg
import asyncio
from discord.ext import commands, tasks
from datetime import datetime
import re
import pytz
import logging
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ConquerTracker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: asyncpg.Pool = bot.db
        self.session: Optional[aiohttp.ClientSession] = None

        self.loop_initialized: bool = False

    async def create_tables(self):
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_settings_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                tribe_id BIGINT NOT NULL,
                starting_unix_timestamp BIGINT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world, tribe_id)
            );
        """)

        await self.db.execute("""
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

        await self.db.execute("""
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

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_world_state_v2 (
                world TEXT PRIMARY KEY,
                last_since BIGINT NOT NULL
            );
        """)

    async def cog_load(self):
        await self.create_tables()

        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

        logger.info("[ConquerTracker] Loaded")

    async def cog_unload(self):
        if self.check_conquers.is_running():
            self.check_conquers.cancel()

        if self.session and not self.session.closed:
            await self.session.close()

        logger.info("[ConquerTracker] Unloaded")

    @commands.Cog.listener()
    async def on_ready(self):
        if self.loop_initialized:
            return

        rows = await self.db.fetch("SELECT 1 FROM conquer_settings_v2 LIMIT 1;")
        if rows and not self.check_conquers.is_running():
            self.check_conquers.start()
            logger.info("[ConquerTracker] Background check_conquers loop started via on_ready.")

        self.loop_initialized = True

    async def get_tribe_id(self, world: str, tribe_tag: str):
        return await self.db.fetchrow("""
            SELECT tribe_id, tag
            FROM ally_data_v3
            WHERE world = $1 AND tag = $2;
        """, world, tribe_tag)

    @commands.command(name="conquertracker")
    @commands.is_owner()
    async def conquertracker(self, ctx: commands.Context, world: str, tribe_tag: str):
        world = world.strip().lower()
        tribe_tag = tribe_tag.strip()
        await self.toggle_tracking(ctx.guild.id, ctx.channel.id, world, tribe_tag)

    async def toggle_tracking(self, guild_id: int, channel_id: int, world: str, tribe_tag: str):
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

        exists = await self.db.fetchval("""
            SELECT 1
            FROM conquer_settings_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4;
        """, guild_id, channel_id, world, tribe_id)

        if exists:
            await self.db.execute("""
                DELETE FROM conquer_settings_v2
                WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4;
            """, guild_id, channel_id, world, tribe_id)

            if channel:
                await channel.send(
                    f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` uitgeschakeld."
                )

            any_left = await self.db.fetchval("SELECT 1 FROM conquer_settings_v2 LIMIT 1;")
            if not any_left and self.check_conquers.is_running():
                self.check_conquers.cancel()

            return False

        now_ts = int(datetime.utcnow().timestamp())

        await self.db.execute("""
            INSERT INTO conquer_settings_v2 (guild_id, channel_id, world, tribe_id, starting_unix_timestamp)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING;
        """, guild_id, channel_id, world, tribe_id, now_ts)

        await self.db.execute("""
            INSERT INTO conquer_world_state_v2 (world, last_since)
            VALUES ($1, $2)
            ON CONFLICT (world) DO NOTHING;
        """, world, now_ts)

        if channel:
            await channel.send(
                f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` aangezet."
            )

        if self.bot.is_ready() and not self.check_conquers.is_running():
            self.check_conquers.start()
            logger.info("[ConquerTracker] Background check_conquers loop started via toggle.")

        return True

    async def _get_since_for_world(self, world: str) -> int:
        row = await self.db.fetchrow("""
            SELECT last_since
            FROM conquer_world_state_v2
            WHERE world = $1;
        """, world)

        if row:
            return int(row["last_since"])

        fallback_since = int(datetime.utcnow().timestamp()) - 3600

        await self.db.execute("""
            INSERT INTO conquer_world_state_v2 (world, last_since)
            VALUES ($1, $2)
            ON CONFLICT (world) DO UPDATE SET last_since = EXCLUDED.last_since;
        """, world, fallback_since)

        return fallback_since

    async def _set_since_for_world(self, world: str, since: int) -> None:
        await self.db.execute("""
            INSERT INTO conquer_world_state_v2 (world, last_since)
            VALUES ($1, $2)
            ON CONFLICT (world) DO UPDATE SET last_since = EXCLUDED.last_since;
        """, world, since)

    @tasks.loop(minutes=1)
    async def check_conquers(self):
        if not self.bot.is_ready():
            return

        tracking_data = await self.db.fetch("""
            SELECT guild_id, channel_id, world, tribe_id
            FROM conquer_settings_v2;
        """)
        if not tracking_data:
            return

        worlds = sorted({row["world"] for row in tracking_data})

        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

        for world in worlds:
            try:
                since = await self._get_since_for_world(world)
                
                now_ts = int(datetime.utcnow().timestamp())
                min_since = now_ts - 84600
                if since < min_since:
                    since = min_since
                
                url = f"https://{world}.tribalwars.nl/interface.php?func=get_conquer_extended&since={since}"

                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            print(f"[ConquerTracker {world.upper()}] HTTP {response.status} bij ophalen conquers")
                            continue
                        raw_data = await response.text()
                except asyncio.TimeoutError:
                    print(f"[ConquerTracker {world.upper()}] timeout bij ophalen conquers")
                    continue
                except aiohttp.ClientError as e:
                    print(f"[ConquerTracker {world.upper()}] aiohttp fout: {e}")
                    continue

                conquers = [e.split(",") for e in re.split(r"\s+", raw_data.strip()) if e]
                if not conquers:
                    print(f"[ConquerTracker {world.upper()}] - 0 new conquers found.")
                    continue

                max_ts_seen = since
                stored_count = 0

                tracking_channels = [t for t in tracking_data if t["world"] == world]
                if not tracking_channels:
                    continue

                for conquer in conquers:
                    if len(conquer) < 4:
                        continue

                    village_id, unix_timestamp, new_owner_id, old_owner_id = map(int, conquer[:4])
                    if unix_timestamp > max_ts_seen:
                        max_ts_seen = unix_timestamp

                    stored = await self.store_conquer(world, village_id, unix_timestamp, new_owner_id, old_owner_id)
                    if not stored:
                        continue

                    stored_count += 1

                    players = await self.db.fetch("""
                        SELECT player_id, tribe_id
                        FROM player_data_v3
                        WHERE world = $1 AND player_id = ANY($2::BIGINT[]);
                    """, world, [new_owner_id, old_owner_id])

                    new_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == new_owner_id), None)
                    old_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == old_owner_id), None)

                    relevant = [
                        t for t in tracking_channels
                        if t["tribe_id"] in (new_owner_tribe_id, old_owner_tribe_id)
                    ]
                    if not relevant:
                        continue

                    for t in relevant:
                        await self.process_conquer(
                            guild_id=t["guild_id"],
                            channel_id=t["channel_id"],
                            world=world,
                            tracked_tribe_id=t["tribe_id"],
                            village_id=village_id,
                            unix_timestamp=unix_timestamp,
                            new_owner_id=new_owner_id,
                            old_owner_id=old_owner_id,
                        )

                await self._set_since_for_world(world, int(max_ts_seen) + 1)

                if stored_count == 0:
                    print(f"[ConquerTracker {world.upper()}] - 0 new conquers found.")
                else:
                    print(f"[ConquerTracker {world.upper()}] - {stored_count} new conquers found.")

            except Exception as e:
                print(f"[ConquerTracker {world.upper()}] scan fout: {e}")

    @check_conquers.before_loop
    async def before_check_conquers(self):
        await self.bot.wait_until_ready()

    async def store_conquer(self, world, village_id, unix_timestamp, new_owner_id, old_owner_id):
        exists = await self.db.fetchval("""
            SELECT 1
            FROM conquer_data_v2
            WHERE world = $1 AND village_id = $2 AND unix_timestamp = $3
              AND new_owner_id = $4 AND old_owner_id = $5;
        """, world, village_id, unix_timestamp, new_owner_id, old_owner_id)
        if exists:
            return False

        players = await self.db.fetch("""
            SELECT player_id, tribe_id
            FROM player_data_v3
            WHERE world = $1 AND player_id = ANY($2::BIGINT[]);
        """, world, [new_owner_id, old_owner_id])

        village = await self.db.fetchrow("""
            SELECT points
            FROM village_data_v3
            WHERE world = $1 AND village_id = $2;
        """, world, village_id)
        if not village:
            return False

        new_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == new_owner_id), None)
        old_owner_tribe_id = next((p["tribe_id"] for p in players if p["player_id"] == old_owner_id), None)

        await self.db.execute("""
            INSERT INTO conquer_data_v2 (
                world, village_id, unix_timestamp,
                new_owner_id, new_owner_tribe_id,
                old_owner_id, old_owner_tribe_id,
                points
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
        """, world, village_id, unix_timestamp, new_owner_id, new_owner_tribe_id, old_owner_id, old_owner_tribe_id, int(village["points"]))

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
        conquer = await self.db.fetchrow("""
            SELECT new_owner_tribe_id, old_owner_tribe_id
            FROM conquer_data_v2
            WHERE world = $1 AND village_id = $2 AND unix_timestamp = $3
              AND new_owner_id = $4 AND old_owner_id = $5;
        """, world, village_id, unix_timestamp, new_owner_id, old_owner_id)
        if not conquer:
            return

        new_owner_tribe_id = conquer["new_owner_tribe_id"]
        old_owner_tribe_id = conquer["old_owner_tribe_id"]

        tribes = await self.db.fetch("""
            SELECT tribe_id, tag
            FROM ally_data_v3
            WHERE world = $1 AND tribe_id = ANY($2::BIGINT[]);
        """, world, [new_owner_tribe_id, old_owner_tribe_id])

        new_owner_tribe_tag = next((t["tag"] for t in tribes if t["tribe_id"] == new_owner_tribe_id), None)
        old_owner_tribe_tag = next((t["tag"] for t in tribes if t["tribe_id"] == old_owner_tribe_id), None)

        players = await self.db.fetch("""
            SELECT player_id, name
            FROM player_data_v3
            WHERE world = $1 AND player_id = ANY($2::BIGINT[]);
        """, world, [new_owner_id, old_owner_id])

        village = await self.db.fetchrow("""
            SELECT name, x, y, points
            FROM village_data_v3
            WHERE world = $1 AND village_id = $2;
        """, world, village_id)
        if not village:
            return

        new_owner = next((p for p in players if p["player_id"] == new_owner_id), None)
        old_owner = next((p for p in players if p["player_id"] == old_owner_id), None)

        new_owner_name = new_owner["name"] if new_owner else "Onbekend"
        old_owner_name = old_owner["name"] if old_owner else "Barbarendorp"
        new_owner_link = f"[{new_owner_name}](https://{world}.tribalwars.nl/game.php?screen=info_player&id={new_owner_id})"
        old_owner_link = f"[{old_owner_name}](https://{world}.tribalwars.nl/game.php?screen=info_player&id={old_owner_id})"
        village_link = f"[{village['name']} ({village['x']}|{village['y']})](https://{world}.tribalwars.nl/game.php?screen=info_village&id={village_id})"

        exists = await self.db.fetchval("""
            SELECT 1
            FROM conquer_messages_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3
              AND village_id = $4 AND unix_timestamp = $5
              AND old_owner_id = $6 AND new_owner_id = $7;
        """, guild_id, channel_id, world, village_id, unix_timestamp, old_owner_id, new_owner_id)
        if exists:
            return

        color = discord.Color.default()
        description = ""

        if new_owner_tribe_id == tracked_tribe_id and old_owner_id == 0:
            description = f"{new_owner_link} heeft een barbarendorp veroverd!"
            color = discord.Color.green()
        elif (
            new_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id == old_owner_tribe_id
            and new_owner_id != old_owner_id
        ):
            description = f"{new_owner_link} heeft een dorp veroverd van zijn of haar stamgenoot {old_owner_link}!"
            color = discord.Color.yellow()
        elif new_owner_tribe_id == tracked_tribe_id and new_owner_id == old_owner_id:
            description = f"{new_owner_link} heeft zichzelf veroverd!"
            color = discord.Color.yellow()
        elif (
            old_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id != old_owner_tribe_id
            and new_owner_tribe_id == 0
        ):
            description = f"{old_owner_link} is een dorp verloren aan {new_owner_link}!"
            color = discord.Color.red()
        elif (
            old_owner_tribe_id == tracked_tribe_id
            and new_owner_tribe_id != old_owner_tribe_id
            and new_owner_tribe_id != 0
        ):
            description = f"{old_owner_link} is een dorp verloren aan {new_owner_link} (`{new_owner_tribe_tag}`)!"
            color = discord.Color.red()
        elif new_owner_tribe_id == tracked_tribe_id and old_owner_tribe_id == 0:
            description = f"{new_owner_link} heeft een dorp veroverd van {old_owner_link}!"
            color = discord.Color.green()
        elif new_owner_tribe_id == tracked_tribe_id and old_owner_tribe_id != 0:
            description = f"{new_owner_link} heeft een dorp veroverd van {old_owner_link} (`{old_owner_tribe_tag}`)!"
            color = discord.Color.green()

        timezone = pytz.timezone("Europe/Amsterdam")
        local_dt = datetime.utcfromtimestamp(unix_timestamp).replace(tzinfo=pytz.utc).astimezone(timezone)
        local_time = local_dt.strftime("%Y-%m-%d %H:%M:%S")

        embed = discord.Embed(description=description, color=color)
        embed.add_field(
            name="Dorp",
            value=village_link,
            inline=True
        )
        embed.add_field(
            name="Punten",
            value=f"```{str(village['points'])}```",
            inline=True
        )
        embed.set_footer(text=f"Tijdstip: {local_time}")

        channel = self.bot.get_channel(channel_id)
        if channel:
            try:
                await channel.send(embed=embed)
            except discord.Forbidden:
                return
            except discord.HTTPException:
                return

        await self.db.execute("""
            INSERT INTO conquer_messages_v2 (
                guild_id, channel_id, world, village_id,
                unix_timestamp, old_owner_id, new_owner_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING;
        """, guild_id, channel_id, world, village_id, unix_timestamp, old_owner_id, new_owner_id)


async def setup(bot: commands.Bot):
    await bot.add_cog(ConquerTracker(bot))
