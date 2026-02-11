import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncpg
from urllib.parse import unquote_plus
import logging
from typing import List, Optional, Any
import aiohttp

from main import create_embed

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WORLDS_API_URL = "https://dkspeed2.jrsoft.tech/api/worlds"


class AcademyTracker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: asyncpg.Pool = self.bot.db
        self.session: Optional[aiohttp.ClientSession] = None
        self.tracked_worlds: set[str] = set()
        self.previous_village_points: dict[str, dict[int, int]] = {}
        self.loop_initialized: bool = False

    async def cog_load(self) -> None:
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS academytracker_channels_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world)
            );
        """)

        rows = await self.db.fetch("SELECT DISTINCT world FROM academytracker_channels_v2;")
        self.tracked_worlds = {row["world"] for row in rows}

        if self.session is None:
            self.session = aiohttp.ClientSession()

        logger.info(f"[AcademyTracker] Loaded with tracked worlds: {self.tracked_worlds}")

    async def cog_unload(self) -> None:
        if self.academy_tracking.is_running():
            self.academy_tracking.cancel()

        if self.session is not None:
            await self.session.close()
            self.session = None

        logger.info("[AcademyTracker] Unloaded")

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        if self.loop_initialized:
            return

        rows = await self.db.fetch("SELECT 1 FROM academytracker_channels_v2 LIMIT 1;")
        if rows and not self.academy_tracking.is_running():
            self.academy_tracking.start()
            logger.info("[AcademyTracker] Background academy_tracking loop started.")

        self.loop_initialized = True

    @tasks.loop(minutes=5)
    async def academy_tracking(self) -> None:
        if not self.tracked_worlds:
            rows = await self.db.fetch("SELECT DISTINCT world FROM academytracker_channels_v2;")
            self.tracked_worlds = {row["world"] for row in rows}
            if not self.tracked_worlds:
                return

        for world in list(self.tracked_worlds):
            try:
                villages = await self.db.fetch("""
                    SELECT village_id, name, x, y, player_id, points
                    FROM village_data_v3
                    WHERE world = $1;
                """, world)

                if world not in self.previous_village_points:
                    self.previous_village_points[world] = {}

                cache = self.previous_village_points[world]

                for village_id, name, x, y, player_id, points in villages:
                    prev_points = cache.get(village_id)

                    if prev_points is not None and prev_points - points == 512:
                        await self.notify_academy_construction(
                            world, village_id, name, x, y, player_id, points
                        )

                    cache[village_id] = points

                print(f"[AcademyTracker {world.upper()}] - Scan completed.")

            except Exception as e:
                logger.error(f"Error in academy tracking for world `{world}`: {e}")

    @academy_tracking.before_loop
    async def before_academy_tracking(self) -> None:
        await self.bot.wait_until_ready()

    async def notify_academy_construction(
        self,
        world: str,
        village_id: int,
        name: str,
        x: int,
        y: int,
        player_id: int,
        points: int
    ) -> None:
        rows = await self.db.fetch("""
            SELECT channel_id
            FROM academytracker_channels_v2
            WHERE world = $1;
        """, world)

        owner_name = "Barbarendorp" if player_id == 0 else await self.get_player_name(world, player_id)
        village_name = self.decode_url(name)
        player_link = f"[{owner_name}](https://{world}.tribalwars.nl/game.php?screen=info_player&id={player_id})" if player_id != 0 else owner_name
        village_link = f"https://{world}.tribalwars.nl/game.php?screen=info_village&id={village_id}"

        for row in rows:
            channel = self.bot.get_channel(row["channel_id"])
            if channel:
                embed = create_embed(description=f"{player_link} heeft een adelshoeve gebouwd")
                embed.color = discord.Color.red()
                embed.add_field(name="Dorp", value=f"```{village_name} ({x}|{y})```", inline=True)
                embed.add_field(name="Punten", value=f"```{points}```", inline=True)
                embed.add_field(name="Link", value=f"[Dorp bekijken]({village_link})", inline=True)
                embed.set_thumbnail(url="https://dsnl.innogamescdn.com/asset/415a0ab7/graphic/big_buildings/snob1.png")

                try:
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    logger.warning(
                        f"[AcademyTracker] Geen toegang tot kanaal {channel.id} in guild {channel.guild.id} (Forbidden). Bericht niet verstuurd."
                    )
                    continue
                except discord.HTTPException as e:
                    logger.warning(
                        f"[AcademyTracker] HTTPException bij versturen naar kanaal {channel.id}: {e}"
                    )
                    continue

    async def get_player_name(self, world: str, player_id: int) -> str:
        result = await self.db.fetchrow("""
            SELECT name
            FROM player_data_v3
            WHERE world = $1 AND player_id = $2;
        """, world, player_id)

        return self.decode_url(result["name"]) if result and result["name"] else "Onbekend"

    def decode_url(self, text: str) -> str:
        return unquote_plus(text)

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AcademyTracker(bot))
