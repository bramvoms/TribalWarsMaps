import discord
import asyncpg
import asyncio
from discord.ext import commands, tasks
from typing import Dict, Tuple, Optional
from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ConquerTrackerV3(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: asyncpg.Pool = self.bot.db
        self.loop_initialized: bool = False

    async def create_tables(self):
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_settings_v3 (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                world TEXT NOT NULL,
                tribe_id BIGINT NOT NULL,
                starting_unix_timestamp BIGINT NOT NULL,
                PRIMARY KEY (guild_id, channel_id, world, tribe_id)
            );
        """)

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS conquer_data_v3 (
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
            CREATE TABLE IF NOT EXISTS conquer_messages_v3 (
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
            CREATE TABLE IF NOT EXISTS conquer_lastowners_v3 (
                world TEXT NOT NULL,
                village_id BIGINT NOT NULL,
                player_id BIGINT NOT NULL,
                tribe_id BIGINT NOT NULL,
                PRIMARY KEY (world, village_id)
            );
        """)

    async def cog_load(self):
        await self.create_tables()

    async def cog_unload(self):
        if self.check_conquers.is_running():
            self.check_conquers.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        if self.loop_initialized:
            return

        rows = await self.db.fetch("SELECT 1 FROM conquer_settings_v3 LIMIT 1;")
        if rows and not self.check_conquers.is_running():
            self.check_conquers.start()
            logger.info("[ConquerTrackerV3] Background check_conquers loop started via on_ready.")

        self.loop_initialized = True

    async def get_tribe_id(self, world: str, tribe_tag: str):
        return await self.db.fetchrow("""
            SELECT tribe_id, tag
            FROM ally_data_v3
            WHERE world = $1 AND tag = $2;
        """, world, tribe_tag)

    @commands.command(name="conquertrackerv3")
    @commands.is_owner()
    async def conquertrackerv3(self, ctx: commands.Context, world: str, tribe_tag: str):
        world = world.strip().lower()
        tribe_tag = tribe_tag.strip()

        await self.toggle_tracking(
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
            world=world,
            tribe_tag=tribe_tag
        )

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
            SELECT 1 FROM conquer_settings_v3
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4;
        """, guild_id, channel_id, world, tribe_id)

        if exists:
            await self.db.execute("""
                DELETE FROM conquer_settings_v3
                WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4;
            """, guild_id, channel_id, world, tribe_id)

            if channel:
                await channel.send(
                    f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` uitgeschakeld."
                )

            any_left = await self.db.fetchval("SELECT 1 FROM conquer_settings_v3;")
            if not any_left and self.check_conquers.is_running():
                self.check_conquers.cancel()

            return False

        await self.db.execute("""
            INSERT INTO conquer_settings_v3 (guild_id, channel_id, world, tribe_id, starting_unix_timestamp)
            VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT);
        """, guild_id, channel_id, world, tribe_id)

        if not await self._world_has_baseline(world):
            await self._ensure_baseline_from_village_data(world)

        if channel:
            await channel.send(
                f"Tracken van veroveringen voor stam `{exact_tag}` op `{world}` aangezet."
            )

        if self.bot.is_ready() and not self.check_conquers.is_running():
            self.check_conquers.start()
            logger.info("[ConquerTrackerV3] Background check_conquers loop started via toggle.")

        return True

    async def _world_has_baseline(self, world: str) -> bool:
        exists = await self.db.fetchval("""
            SELECT 1
            FROM conquer_lastowners_v3
            WHERE world = $1
            LIMIT 1;
        """, world)
        return bool(exists)

    async def _ensure_baseline_from_village_data(self, world: str) -> None:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO conquer_lastowners_v3 (world, village_id, player_id, tribe_id)
                    SELECT world, village_id, player_id, tribe_id
                    FROM village_data_v3
                    WHERE world = $1
                    ON CONFLICT (world, village_id) DO UPDATE
                    SET player_id = EXCLUDED.player_id,
                        tribe_id = EXCLUDED.tribe_id;
                """, world)

    async def _load_lastowners_for_world(self, world: str) -> Dict[int, Tuple[int, int]]:
        rows = await self.db.fetch("""
            SELECT village_id, player_id, tribe_id
            FROM conquer_lastowners_v3
            WHERE world = $1;
        """, world)

        out: Dict[int, Tuple[int, int]] = {}
        for r in rows:
            out[int(r["village_id"])] = (int(r["player_id"]), int(r["tribe_id"]))
        return out

    async def _upsert_lastowners_for_world(self, world: str, current: Dict[int, Tuple[int, int]]):
        if not current:
            return

        async with self.db.acquire() as conn:
            async with conn.transaction():
                for vid, (pid, tid) in current.items():
                    await conn.execute("""
                        INSERT INTO conquer_lastowners_v3 (world, village_id, player_id, tribe_id)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (world, village_id) DO UPDATE
                        SET player_id = EXCLUDED.player_id,
                            tribe_id = EXCLUDED.tribe_id;
                    """, world, vid, pid, tid)

    async def _fetch_current_villages_world(self, world: str) -> Dict[int, Dict[str, int]]:
        rows = await self.db.fetch("""
            SELECT village_id, player_id, tribe_id, points
            FROM village_data_v3
            WHERE world = $1;
        """, world)

        out: Dict[int, Dict[str, int]] = {}
        for r in rows:
            vid = int(r["village_id"])
            out[vid] = {
                "player_id": int(r["player_id"]),
                "tribe_id": int(r["tribe_id"]),
                "points": int(r["points"]),
            }
        return out

    @tasks.loop(seconds=30)
    async def check_conquers(self):
        logger.info("[ConquerTrackerV3] tick")

        if not self.bot.is_ready():
            logger.info("[ConquerTrackerV3] bot not ready")
            return

        tracking_data = await self.db.fetch("SELECT * FROM conquer_settings_v3;")
        logger.info("[ConquerTrackerV3] settings rows=%s", len(tracking_data))

        if not tracking_data:
            return

        worlds = sorted({row["world"] for row in tracking_data})
        logger.info("[ConquerTrackerV3] worlds=%s", worlds)

        for world in worlds:
            try:
                current = await self._fetch_current_villages_world(world)
                logger.info(
                    "[ConquerTrackerV3 %s] current villages=%s",
                    world.upper(),
                    len(current),
                )

                if not current:
                    continue

                baseline_exists = await self._world_has_baseline(world)
                logger.info(
                    "[ConquerTrackerV3 %s] baseline_exists=%s",
                    world.upper(),
                    baseline_exists,
                )

                if not baseline_exists:
                    await self._ensure_baseline_from_village_data(world)
                    logger.info(
                        "[ConquerTrackerV3 %s] baseline inserted from village_data_v3",
                        world.upper(),
                    )
                    continue

                t0 = datetime.utcnow()
                lastowners = await self._load_lastowners_for_world(world)
                logger.info(
                    "[ConquerTrackerV3 %s] loaded lastowners=%s in %.2fs",
                    world.upper(),
                    len(lastowners),
                    (datetime.utcnow() - t0).total_seconds(),
                )

                now_ts = int(datetime.utcnow().timestamp())

                tracking_channels = await self.db.fetch(
                    """
                    SELECT guild_id, channel_id, tribe_id
                    FROM conquer_settings_v3
                    WHERE world = $1;
                    """,
                    world,
                )

                changed_lastowners: Dict[int, Tuple[int, int]] = {}

                for village_id, cur in current.items():
                    new_owner_id = int(cur["player_id"])
                    new_owner_tribe_id = int(cur["tribe_id"])
                    points = int(cur["points"])

                    prev = lastowners.get(village_id)
                    if not prev:
                        changed_lastowners[village_id] = (new_owner_id, new_owner_tribe_id)
                        continue

                    old_owner_id, old_owner_tribe_id = prev

                    if new_owner_id == old_owner_id and new_owner_tribe_id == old_owner_tribe_id:
                        continue

                    changed_lastowners[village_id] = (new_owner_id, new_owner_tribe_id)

                    if new_owner_id == old_owner_id:
                        continue

                    stored = await self.store_conquer(
                        world=world,
                        village_id=village_id,
                        unix_timestamp=now_ts,
                        new_owner_id=new_owner_id,
                        old_owner_id=old_owner_id,
                        new_owner_tribe_id=new_owner_tribe_id,
                        old_owner_tribe_id=old_owner_tribe_id,
                        points=points,
                    )
                    if not stored:
                        continue

                    relevant_channels = [
                        t
                        for t in tracking_channels
                        if int(t["tribe_id"]) in (new_owner_tribe_id, old_owner_tribe_id)
                    ]

                    for tracking in relevant_channels:
                        await self.process_conquer(
                            guild_id=int(tracking["guild_id"]),
                            channel_id=int(tracking["channel_id"]),
                            world=world,
                            tracked_tribe_id=int(tracking["tribe_id"]),
                            village_id=village_id,
                            unix_timestamp=now_ts,
                            new_owner_id=new_owner_id,
                            old_owner_id=old_owner_id,
                        )

                t1 = datetime.utcnow()
                if changed_lastowners:
                    await self._upsert_lastowners_for_world(world, changed_lastowners)
                    logger.info(
                        "[ConquerTrackerV3 %s] lastowners updated (%s villages) in %.2fs",
                        world.upper(),
                        len(changed_lastowners),
                        (datetime.utcnow() - t1).total_seconds(),
                    )
                else:
                    logger.info("[ConquerTrackerV3 %s] lastowners updated (0 villages)", world.upper())

                logger.info("[ConquerTrackerV3 %s] scan completed", world.upper())

            except Exception:
                logger.exception(
                    "[ConquerTrackerV3 %s] error during scan",
                    world.upper(),
                )

    @check_conquers.before_loop
    async def before_check_conquers(self):
        await self.bot.wait_until_ready()

    async def store_conquer(
        self,
        world: str,
        village_id: int,
        unix_timestamp: int,
        new_owner_id: int,
        old_owner_id: int,
        new_owner_tribe_id: Optional[int],
        old_owner_tribe_id: Optional[int],
        points: int,
    ):
        exists = await self.db.fetchval("""
            SELECT 1 FROM conquer_data_v3
            WHERE world = $1 AND village_id = $2 AND unix_timestamp = $3
              AND new_owner_id = $4 AND old_owner_id = $5;
        """, world, village_id, unix_timestamp, new_owner_id, old_owner_id)

        if exists:
            return False

        await self.db.execute("""
            INSERT INTO conquer_data_v3 (
                world, village_id, unix_timestamp,
                new_owner_id, new_owner_tribe_id,
                old_owner_id, old_owner_tribe_id,
                points
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
        """, world, village_id, unix_timestamp, new_owner_id, new_owner_tribe_id, old_owner_id, old_owner_tribe_id, points)

        return True

    async def process_conquer(
        self,
        guild_id: int,
        channel_id: int,
        world: str,
        tracked_tribe_id: int,
        village_id: int,
        unix_timestamp: int,
        new_owner_id: int,
        old_owner_id: int
    ):
        conquer = await self.db.fetchrow("""
            SELECT new_owner_tribe_id, old_owner_tribe_id
            FROM conquer_data_v3
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
            WHERE world = $1 AND tribe_id IN ($2, $3);
        """, world, new_owner_tribe_id, old_owner_tribe_id)

        new_owner_tribe_tag = next((t["tag"] for t in tribes if t["tribe_id"] == new_owner_tribe_id), None)
        old_owner_tribe_tag = next((t["tag"] for t in tribes if t["tribe_id"] == old_owner_tribe_id), None)

        players = await self.db.fetch("""
            SELECT player_id, name
            FROM player_data_v3
            WHERE world = $1 AND player_id IN ($2, $3);
        """, world, new_owner_id, old_owner_id)

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

        exists = await self.db.fetchval("""
            SELECT 1 FROM conquer_messages_v3
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3
              AND village_id = $4 AND unix_timestamp = $5
              AND old_owner_id = $6 AND new_owner_id = $7;
        """, guild_id, channel_id, world, village_id, unix_timestamp, old_owner_id, new_owner_id)

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
                f"**{new_owner_name}** heeft een dorp veroverd van zijn of haar stamgenoot "
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
            description = f"**{new_owner_name}** heeft een dorp veroverd van **{old_owner_name}**!"
            color = discord.Color.green()
        elif new_owner_tribe_id == tracked_tribe_id and old_owner_tribe_id != 0:
            description = (
                f"**{new_owner_name}** heeft een dorp veroverd van **{old_owner_name}** "
                f"(`{old_owner_tribe_tag}`)!"
            )
            color = discord.Color.green()

        timezone = pytz.timezone("Europe/Amsterdam")
        local_dt = datetime.utcfromtimestamp(unix_timestamp).replace(tzinfo=pytz.utc).astimezone(timezone)
        local_time = local_dt.strftime("%Y-%m-%d %H:%M:%S")

        embed = discord.Embed(description=description, color=color)
        embed.add_field(
            name="Dorp",
            value=f"```{village['name']} ({village['x']}|{village['y']})```",
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
                await asyncio.sleep(1)
            except discord.Forbidden:
                return
            except discord.HTTPException:
                return

        await self.db.execute("""
            INSERT INTO conquer_messages_v3 (
                guild_id, channel_id, world, village_id,
                unix_timestamp, old_owner_id, new_owner_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING;
        """, guild_id, channel_id, world, village_id, unix_timestamp, old_owner_id, new_owner_id)


async def setup(bot: commands.Bot):
    await bot.add_cog(ConquerTrackerV3(bot))
