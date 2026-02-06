import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncpg
import logging
import requests
import aiohttp
import asyncio
from datetime import datetime
from typing import Optional
from main import create_embed

logger = logging.getLogger(__name__)

KILL_TYPES = {
    "kill_att": "ODA",
    "kill_def": "ODD",
    "kill_sup": "ODS"
}

class ODTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = self.bot.db
        self.loop_initialized = False
        self._last_channel_send = {}

    async def cog_load(self):
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS odtracker_configs_v2 (
                world TEXT PRIMARY KEY
            );
        """)

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS odtracker_enabled_tribes_v2 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                tribe_tag TEXT NOT NULL,
                min_threshold BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, channel_id, world, tribe_tag)
            );
        """)

        await self.db.execute("""
            ALTER TABLE odtracker_enabled_tribes_v2
            ADD COLUMN IF NOT EXISTS min_threshold BIGINT NOT NULL DEFAULT 0;
        """)

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS odtracker_data_v2 (
                world TEXT NOT NULL,
                player_id BIGINT NOT NULL,
                kill_att BIGINT DEFAULT 0,
                kill_def BIGINT DEFAULT 0,
                kill_sup BIGINT DEFAULT 0,
                cooldown_att TIMESTAMP DEFAULT NULL,
                cooldown_def TIMESTAMP DEFAULT NULL,
                cooldown_sup TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (world, player_id)
            );
        """)

    async def initial_scan_world(self, world: str):
        results = {}

        for kill_type in KILL_TYPES:
            try:
                url = f"https://{world}.tribalwars.nl/map/{kill_type}.txt"
                response = requests.get(url)
                if response.status_code == 200:
                    for line in response.text.strip().splitlines():
                        _, player_id, kills = line.split(',')
                        player_id = int(player_id)
                        kills = int(kills)
                        if player_id not in results:
                            results[player_id] = {}
                        results[player_id][kill_type] = kills
            except Exception as e:
                logger.warning(f"Error fetching {kill_type} for {world}: {e}")

        if results:
            await self.update_od_database(world, results)

    @tasks.loop(hours=24)
    async def cleanup_odtracker(self):
        """Verwijder spelers uit odtracker_data_v2 die niet meer bestaan in player_data_v3."""
        async with self.db.acquire() as conn:
            rows = await conn.fetch("SELECT world FROM odtracker_configs_v2")
            for row in rows:
                world = row["world"]

                try:
                    player_ids = await conn.fetch("""
                        SELECT player_id FROM player_data_v3
                        WHERE world = $1
                    """, world)
                    existing_ids = {r["player_id"] for r in player_ids}

                    tracked_ids = await conn.fetch("""
                        SELECT player_id FROM odtracker_data_v2
                        WHERE world = $1
                    """, world)
                    current_ids = {r["player_id"] for r in tracked_ids}

                    to_delete = current_ids - existing_ids
                    if to_delete:
                        await conn.execute("""
                            DELETE FROM odtracker_data_v2
                            WHERE world = $1 AND player_id = ANY($2::bigint[])
                        """, world, list(to_delete))
                        print(f"[ODTracker {world.upper()}] - Cleanup removed {len(to_delete)} players.")
                    else:
                        print(f"[ODTracker {world.upper()}] - Cleanup: nothing to remove.")
                except Exception as e:
                    logger.exception(f"Error during ODTracker cleanup for {world}: {e}")

    @tasks.loop(minutes=5)
    async def scan_od(self):
        rows = await self.db.fetch("SELECT world FROM odtracker_configs_v2")
        for row in rows:
            world = row['world']

            results = {}
            for kill_type in KILL_TYPES:
                try:
                    url = f"https://{world}.tribalwars.nl/map/{kill_type}.txt"
                    response = requests.get(url)
                    if response.status_code == 200:
                        for line in response.text.strip().splitlines():
                            _, player_id, kills = line.split(',')
                            player_id = int(player_id)
                            kills = int(kills)
                            if player_id not in results:
                                results[player_id] = {}
                            results[player_id][kill_type] = kills
                except Exception as e:
                    logger.warning(f"Error fetching {kill_type} for {world}: {e}")

            await self.update_od_database(world, results)
            print(f"[ODTracker {world.upper()}] - Scan completed.")

    async def update_od_database(self, world, results):
        async with self.db.acquire() as conn:
            for player_id, data in results.items():
                previous = await conn.fetchrow("""
                    SELECT kill_att, kill_def, kill_sup, cooldown_att, cooldown_def, cooldown_sup
                    FROM odtracker_data_v2
                    WHERE world = $1 AND player_id = $2
                """, world, player_id)

                increases = {}
                for key in KILL_TYPES:
                    new_val = data.get(key, 0)
                    old_val = previous[key] if previous and previous[key] is not None else 0
                    if new_val > old_val:
                        increases[key] = {
                            "delta": new_val - old_val,
                            "old": old_val,
                            "new": new_val
                        }

                if not increases:
                    continue

                now = datetime.utcnow()

                updated_kills = {"kill_att": None, "kill_def": None, "kill_sup": None}
                updated_cooldowns = {"cooldown_att": None, "cooldown_def": None, "cooldown_sup": None}

                for key in list(increases.keys()):
                    cooldown_field = f"cooldown_{key.split('_')[1]}"
                    last_ts = previous[cooldown_field] if previous and previous[cooldown_field] is not None else None
                    if not last_ts or (now - last_ts).total_seconds() >= 3600:
                        updated_kills[key] = data.get(key, 0)
                        updated_cooldowns[cooldown_field] = now
                    else:
                        increases.pop(key, None)

                if not increases:
                    continue

                await conn.execute("""
                    INSERT INTO odtracker_data_v2 (
                        world, player_id, kill_att, kill_def, kill_sup,
                        cooldown_att, cooldown_def, cooldown_sup
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (world, player_id) DO UPDATE SET
                        kill_att = COALESCE(EXCLUDED.kill_att, odtracker_data_v2.kill_att),
                        kill_def = COALESCE(EXCLUDED.kill_def, odtracker_data_v2.kill_def),
                        kill_sup = COALESCE(EXCLUDED.kill_sup, odtracker_data_v2.kill_sup),
                        cooldown_att = COALESCE(EXCLUDED.cooldown_att, odtracker_data_v2.cooldown_att),
                        cooldown_def = COALESCE(EXCLUDED.cooldown_def, odtracker_data_v2.cooldown_def),
                        cooldown_sup = COALESCE(EXCLUDED.cooldown_sup, odtracker_data_v2.cooldown_sup)
                """, world, player_id,
                     updated_kills["kill_att"], updated_kills["kill_def"], updated_kills["kill_sup"],
                     updated_cooldowns["cooldown_att"], updated_cooldowns["cooldown_def"], updated_cooldowns["cooldown_sup"])

                await self.notify_increase(conn, world, player_id, increases)

    async def notify_increase(self, conn, world, player_id, increases):
        player = await conn.fetchrow("""
            SELECT name, tribe_id FROM player_data_v3
            WHERE world = $1 AND player_id = $2
        """, world, player_id)
        if not player:
            return

        tribe = await conn.fetchrow("""
            SELECT tag FROM ally_data_v3
            WHERE world = $1 AND tribe_id = $2
        """, world, player['tribe_id'])
        tribe_tag = tribe['tag'] if tribe else None

        channels = await conn.fetch("""
            SELECT channel_id, min_threshold
            FROM odtracker_enabled_tribes_v2
            WHERE world = $1 AND (tribe_tag = $2 OR tribe_tag = 'alltribes')
        """, world, tribe_tag)

        for row in channels:
            channel = self.bot.get_channel(row['channel_id'])
            if not channel:
                continue

            channel_min = row["min_threshold"]

            for key, data in increases.items():
                kill_type = KILL_TYPES[key]
                delta = data["delta"]

                if delta < channel_min:
                    continue

                old_val = data["old"]
                new_val = data["new"]

                if tribe_tag:
                    description = f"**{kill_type}** van **{player['name']}** ({tribe_tag}) is gestegen"
                else:
                    description = f"**{kill_type}** van **{player['name']}** is gestegen"

                embed = create_embed(description=description)
                embed.color = discord.Color.green()
                embed.add_field(name="Stijging", value=f"```+{delta:,}```".replace(",", "."), inline=False)
                embed.add_field(name="Oude score", value=f"```{old_val:,}```".replace(",", "."), inline=True)
                embed.add_field(name="Nieuwe score", value=f"```{new_val:,}```".replace(",", "."), inline=True)
                embed.set_thumbnail(url="https://dsnl.innogamescdn.com/asset/415a0ab7/graphic/awards/progress/kills.png")

                now_ts = datetime.utcnow().timestamp()
                last_ts = self._last_channel_send.get(channel.id)
                if last_ts is not None:
                    diff = now_ts - last_ts
                    if diff < 1.0:
                        await asyncio.sleep(1.0 - diff)

                self._last_channel_send[channel.id] = datetime.utcnow().timestamp()
                try:
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    logger.warning(
                        f"[ODTracker] Geen toegang tot kanaal {channel.id} in guild {channel.guild.id} (Forbidden). Bericht niet verstuurd."
                    )
                    continue
                except discord.HTTPException as e:
                    logger.warning(
                        f"[ODTracker] HTTPException bij versturen naar kanaal {channel.id}: {e}"
                    )
                    continue

    @scan_od.before_loop
    async def before_scan_od(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        if self.loop_initialized:
            return

        rows = await self.db.fetch("SELECT world FROM odtracker_configs_v2")
        if rows and not self.scan_od.is_running():
            self.scan_od.start()
            print("[ODTracker] Background scan loop started.")

        if not self.cleanup_odtracker.is_running():
            self.cleanup_odtracker.start()

        self.loop_initialized = True


async def setup(bot):
    cog = ODTracker(bot)
    await cog.cog_load()
    await bot.add_cog(cog)
