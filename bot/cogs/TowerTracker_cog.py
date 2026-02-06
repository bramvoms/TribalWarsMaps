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

class TowerTracker(commands.Cog):
    WATCHTOWER_LEVELS = {
        155: 18,
        186: 19,
        224: 20,
    }

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: asyncpg.Pool = bot.db

        self.tracked_worlds: set[str] = set()
        self.previous_village_points: dict[str, dict[int, int]] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        
        self.loop_initialized: bool = False

    async def cog_load(self) -> None:
        """Runs when the cog is loaded."""
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS towertracker_channels_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world)
            );
        """)

        rows = await self.db.fetch("SELECT DISTINCT world FROM towertracker_channels_v2;")
        self.tracked_worlds = {row["world"] for row in rows}

        if self.session is None:
            self.session = aiohttp.ClientSession()

        logger.info(f"[TowerTracker] Loaded with tracked worlds: {self.tracked_worlds}")

    async def cog_unload(self) -> None:
        """Stop the task and properly close resources."""
        if self.tower_tracking.is_running():
            self.tower_tracking.cancel()
        if self.session is not None:
            await self.session.close()
            self.session = None

        logger.info("[TowerTracker] Unloaded")

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        if self.loop_initialized:
            return

        rows = await self.db.fetch("SELECT 1 FROM towertracker_channels_v2 LIMIT 1;")
        if rows and not self.tower_tracking.is_running():
            self.tower_tracking.start()
            logger.info("[TowerTracker] Background tower_tracking loop started.")

        self.loop_initialized = True

    @tasks.loop(minutes=5)
    async def tower_tracking(self) -> None:
        """Task to scan the village_data_v3 table for watchtower constructions."""
        if not self.tracked_worlds:
            rows = await self.db.fetch("SELECT DISTINCT world FROM towertracker_channels_v2;")
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

                world_cache = self.previous_village_points[world]

                for village_id, name, x, y, player_id, points in villages:
                    prev_points = world_cache.get(village_id)

                    if prev_points is not None:
                        point_gain = points - prev_points
                        if points >= 1200 and point_gain in self.WATCHTOWER_LEVELS:
                            level = self.WATCHTOWER_LEVELS[point_gain]
                            await self.notify_tower_construction(
                                world, village_id, name, x, y, player_id, level
                            )

                    world_cache[village_id] = points

                print(f"[TowerTracker {world.upper()}] - Scan completed.")

            except Exception as e:
                logger.error(f"Error processing tower tracking for world `{world}`: {e}")

    @tower_tracking.before_loop
    async def before_tower_tracking(self) -> None:
        await self.bot.wait_until_ready()

    async def notify_tower_construction(
        self,
        world: str,
        village_id: int,
        name: str,
        x: int,
        y: int,
        player_id: int,
        level: int
    ) -> None:
        """Send a notification about a confirmed watchtower construction."""
        rows = await self.db.fetch("""
            SELECT channel_id
            FROM towertracker_channels_v2
            WHERE world = $1;
        """, world)

        owner_name = "Barbarendorp" if player_id == 0 else await self.get_player_name(world, player_id)
        village_name = self.decode_url(name)

        for row in rows:
            channel = self.bot.get_channel(row["channel_id"])
            if channel:
                title = f"Uitkijktoren gebouwd op {world.upper()}"
                embed = create_embed(title=title, description=None)
                embed.add_field(name="Dorp", value=f"```{village_name} ({x}|{y})```", inline=True)
                embed.add_field(name="Eigenaar", value=f"```{owner_name}```", inline=True)
                embed.add_field(name="Uitkijktoren level", value=f"```{level}```", inline=True)
                embed.set_thumbnail(url="https://dsnl.innogamescdn.com/asset/415a0ab7/graphic/big_buildings/watchtower3.png")

                try:
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    continue
                except discord.HTTPException as e:
                    continue

    async def get_player_name(self, world: str, player_id: int) -> str:
        """Fetch the player name for a given player ID."""
        result = await self.db.fetchrow("""
            SELECT name
            FROM player_data_v3
            WHERE world = $1 AND player_id = $2;
        """, world, player_id)
        return self.decode_url(result["name"]) if result and result["name"] else "Onbekend"

    def decode_url(self, text: str) -> str:
        """Decode URL-encoded strings."""
        return unquote_plus(text)

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(TowerTracker(bot))
