import discord
from discord.ext import commands
from discord import app_commands
import asyncpg
import logging
from typing import List, Tuple, Optional

from main import create_embed

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ToggleTrackers(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: asyncpg.Pool = bot.db

    # ---------------------- Start UI ---------------------- #

    def _start_embed(self) -> discord.Embed:
        title = "Tracker aanpassen"
        description = "Welke tracker wil je aanpassen?"
        return create_embed(title=title, description=description)

    def _tracker_embed(self, tracker_id: str) -> discord.Embed:
        title = "Tracker aanpassen"
        description = f"Wil je de **{self._fmt_tracker_label(tracker_id)}** tracker aan of uit zetten?"
        return create_embed(title=title, description=description)

    # ---------------------- Config ---------------------- #

    def _get_tracker_configs(self) -> dict:
        return {
            "academy": {
                "label": "Adelshoeve",
                "table": "academytracker_channels_v2",
                "cog_name": "AcademyTracker",
            },
            "wall": {
                "label": "Muur",
                "table": "walltracker_channels_maps_v2",
                "cog_name": "WallTracker",
            },
            "tower": {
                "label": "Uitkijktoren",
                "table": "towertracker_channels_v2",
                "cog_name": "TowerTracker",
            },
            "conquer": {
                "label": "Veroveringen",
                "table": "conquer_settings_v2",
                "cog_name": "ConquerTracker",
            },
            "od": {
                "label": "OD",
                "table": "odtracker_enabled_tribes_v2",
                "cog_name": "ODTracker",
            },
        }

    def _get_tracker_cog(self, tracker_id: str):
        cfg = self._get_tracker_configs()[tracker_id]
        return self.bot.get_cog(cfg["cog_name"])

    def _fmt_tracker_label(self, tracker_id: str) -> str:
        label = self._get_tracker_configs()[tracker_id]["label"]
        if tracker_id == "od":
            return label.upper()
        return label.lower()

    # ---------------------- Worlds (villagedata_worlds) ---------------------- #

    async def _fetch_worlds_from_villagedata(self) -> List[str]:
        rows = await self.db.fetch(
            """
            SELECT world
            FROM villagedata_worlds
            ORDER BY world;
            """
        )
        worlds = [r["world"] for r in rows if r.get("world")]
        worlds = [w for w in worlds if isinstance(w, str) and w.startswith("nl")]
        return sorted(set(worlds))

    async def _world_is_enabled_villagedata(self, world: str) -> bool:
        exists = await self.db.fetchval(
            "SELECT 1 FROM villagedata_worlds WHERE world = $1;",
            world
        )
        return bool(exists)

    # ---------------------- Simple trackers DB helpers ---------------------- #

    async def _fetch_enabled_worlds_in_channel_simple(
        self,
        tracker_id: str,
        guild_id: int,
        channel_id: int
    ) -> List[str]:
        cfg = self._get_tracker_configs()[tracker_id]
        table = cfg["table"]
        rows = await self.db.fetch(
            f"""
            SELECT world
            FROM {table}
            WHERE guild_id = $1 AND channel_id = $2
            ORDER BY world;
            """,
            guild_id, channel_id
        )
        worlds = [r["world"] for r in rows if r.get("world")]
        return sorted(set(worlds))

    async def _is_tracker_enabled_in_channel_simple(
        self,
        tracker_id: str,
        guild_id: int,
        channel_id: int,
        world: str
    ) -> bool:
        cfg = self._get_tracker_configs()[tracker_id]
        table = cfg["table"]

        exists = await self.db.fetchval(
            f"""
            SELECT 1
            FROM {table}
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3
            LIMIT 1;
            """,
            guild_id, channel_id, world
        )
        return bool(exists)

    async def _enable_tracker_in_channel_simple(
        self,
        tracker_id: str,
        guild_id: int,
        channel_id: int,
        world: str
    ) -> None:
        cfg = self._get_tracker_configs()[tracker_id]
        table = cfg["table"]

        await self.db.execute(
            f"""
            INSERT INTO {table} (guild_id, channel_id, world)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING;
            """,
            guild_id, channel_id, world
        )

        tracker_cog = self._get_tracker_cog(tracker_id)
        if tracker_cog is not None and hasattr(tracker_cog, "tracked_worlds"):
            try:
                tracker_cog.tracked_worlds.add(world)
            except Exception:
                pass

    async def _disable_tracker_in_channel_simple(
        self,
        tracker_id: str,
        guild_id: int,
        channel_id: int,
        world: str
    ) -> None:
        cfg = self._get_tracker_configs()[tracker_id]
        table = cfg["table"]

        await self.db.execute(
            f"""
            DELETE FROM {table}
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3;
            """,
            guild_id, channel_id, world
        )

        still_exists = await self.db.fetchval(
            f"""
            SELECT 1
            FROM {table}
            WHERE world = $1
            LIMIT 1;
            """,
            world
        )

        if not still_exists:
            tracker_cog = self._get_tracker_cog(tracker_id)
            if tracker_cog is not None and hasattr(tracker_cog, "tracked_worlds"):
                try:
                    tracker_cog.tracked_worlds.discard(world)
                except Exception:
                    pass

    # ---------------------- Conquer helpers ---------------------- #

    async def _conquer_world_is_enabled(self, world: str) -> bool:
        exists_world = await self.db.fetchval(
            "SELECT 1 FROM ally_data WHERE world = $1 LIMIT 1;",
            world
        )
        return bool(exists_world)

    async def _conquer_fetch_enabled_in_channel(self, guild_id: int, channel_id: int) -> List[Tuple[str, int, str]]:
        rows = await self.db.fetch(
            """
            SELECT c.world, c.tribe_id, a.tag
            FROM conquer_settings_v2 c
            JOIN ally_data a
              ON a.world = c.world
             AND a.tribe_id = c.tribe_id
            WHERE c.guild_id = $1 AND c.channel_id = $2
            ORDER BY c.world, a.tag;
            """,
            guild_id, channel_id
        )
        result: List[Tuple[str, int, str]] = []
        for r in rows:
            if r.get("world") and r.get("tribe_id") is not None and r.get("tag"):
                result.append((r["world"], int(r["tribe_id"]), r["tag"]))
        return result

    async def _conquer_fetch_tribes_for_world(self, world: str, search: str = "") -> List[Tuple[int, str]]:
        search_text = (search or "").strip()
        if search_text:
            rows = await self.db.fetch(
                """
                SELECT DISTINCT tribe_id, tag
                FROM ally_data
                WHERE world = $1 AND tag ILIKE $2
                ORDER BY tag
                LIMIT 25;
                """,
                world, f"%{search_text}%"
            )
        else:
            rows = await self.db.fetch(
                """
                SELECT DISTINCT tribe_id, tag
                FROM ally_data
                WHERE world = $1
                ORDER BY tag
                LIMIT 25;
                """,
                world
            )

        out: List[Tuple[int, str]] = []
        for r in rows:
            if r.get("tribe_id") is not None and r.get("tag"):
                out.append((int(r["tribe_id"]), r["tag"]))
        return out

    async def _conquer_is_enabled(self, guild_id: int, channel_id: int, world: str, tribe_id: int) -> bool:
        exists = await self.db.fetchval(
            """
            SELECT 1 FROM conquer_settings_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4
            LIMIT 1;
            """,
            guild_id, channel_id, world, tribe_id
        )
        return bool(exists)

    async def _conquer_enable(self, guild_id: int, channel_id: int, world: str, tribe_id: int) -> None:
        await self.db.execute(
            """
            INSERT INTO conquer_settings_v2 (guild_id, channel_id, world, tribe_id, starting_unix_timestamp)
            VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
            ON CONFLICT DO NOTHING;
            """,
            guild_id, channel_id, world, tribe_id
        )

        conquer_cog = self._get_tracker_cog("conquer")
        if conquer_cog is not None and hasattr(conquer_cog, "check_conquers"):
            try:
                if not conquer_cog.check_conquers.is_running():
                    conquer_cog.check_conquers.start()
            except Exception:
                pass

    async def _conquer_disable(self, guild_id: int, channel_id: int, world: str, tribe_id: int) -> None:
        await self.db.execute(
            """
            DELETE FROM conquer_settings_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_id = $4;
            """,
            guild_id, channel_id, world, tribe_id
        )

        still_any = await self.db.fetchval("SELECT 1 FROM conquer_settings_v2 LIMIT 1;")
        if not still_any:
            conquer_cog = self._get_tracker_cog("conquer")
            if conquer_cog is not None and hasattr(conquer_cog, "check_conquers"):
                try:
                    if conquer_cog.check_conquers.is_running():
                        conquer_cog.check_conquers.cancel()
                except Exception:
                    pass

    # ---------------------- OD helpers ---------------------- #

    async def _od_fetch_tags_for_world(self, world: str, search: str = "") -> List[str]:
        search_text = (search or "").strip()
        if search_text:
            rows = await self.db.fetch(
                """
                SELECT DISTINCT tag
                FROM ally_data
                WHERE world = $1 AND tag ILIKE $2
                ORDER BY tag
                LIMIT 25;
                """,
                world, f"%{search_text}%"
            )
        else:
            rows = await self.db.fetch(
                """
                SELECT DISTINCT tag
                FROM ally_data
                WHERE world = $1
                ORDER BY tag
                LIMIT 25;
                """,
                world
            )

        tags = [r["tag"] for r in rows if r.get("tag")]
        return tags[:25]

    async def _od_is_enabled(self, guild_id: int, channel_id: int, world: str, tribe_tag: str) -> bool:
        exists = await self.db.fetchval(
            """
            SELECT 1
            FROM odtracker_enabled_tribes_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_tag = $4
            LIMIT 1;
            """,
            guild_id, channel_id, world, tribe_tag
        )
        return bool(exists)

    async def _od_fetch_enabled_in_channel(self, guild_id: int, channel_id: int) -> List[Tuple[str, str]]:
        rows = await self.db.fetch(
            """
            SELECT world, tribe_tag
            FROM odtracker_enabled_tribes_v2
            WHERE guild_id = $1 AND channel_id = $2
            ORDER BY world, tribe_tag;
            """,
            guild_id, channel_id
        )
        entries: List[Tuple[str, str]] = []
        for r in rows:
            if r.get("world") and r.get("tribe_tag"):
                entries.append((r["world"], r["tribe_tag"]))
        return entries

    async def _od_enable(self, guild_id: int, channel_id: int, world: str, tribe_tag: str) -> None:
        od_cog = self._get_tracker_cog("od")

        exists_cfg = await self.db.fetchval(
            "SELECT 1 FROM odtracker_configs_v2 WHERE world = $1 LIMIT 1;",
            world
        )
        if not exists_cfg:
            await self.db.execute(
                "INSERT INTO odtracker_configs_v2 (world) VALUES ($1);",
                world
            )
            if od_cog is not None and hasattr(od_cog, "initial_scan_world"):
                try:
                    await od_cog.initial_scan_world(world)
                except Exception:
                    logger.exception("[ToggleTrackers] OD initial_scan_world failed")

        await self.db.execute(
            """
            INSERT INTO odtracker_enabled_tribes_v2 (
                guild_id, channel_id, world, tribe_tag, min_threshold
            )
            VALUES ($1, $2, $3, $4, 0)
            ON CONFLICT (guild_id, channel_id, world, tribe_tag) DO UPDATE
            SET min_threshold = EXCLUDED.min_threshold;
            """,
            guild_id, channel_id, world, tribe_tag
        )

        if od_cog is not None:
            try:
                if hasattr(od_cog, "scan_od") and not od_cog.scan_od.is_running():
                    od_cog.scan_od.start()
                if hasattr(od_cog, "cleanup_odtracker") and not od_cog.cleanup_odtracker.is_running():
                    od_cog.cleanup_odtracker.start()
            except Exception:
                logger.exception("[ToggleTrackers] OD loops start failed")

    async def _od_disable(self, guild_id: int, channel_id: int, world: str, tribe_tag: str) -> None:
        await self.db.execute(
            """
            DELETE FROM odtracker_enabled_tribes_v2
            WHERE guild_id = $1 AND channel_id = $2 AND world = $3 AND tribe_tag = $4;
            """,
            guild_id, channel_id, world, tribe_tag
        )

        still_for_world = await self.db.fetchval(
            """
            SELECT 1
            FROM odtracker_enabled_tribes_v2
            WHERE world = $1
            LIMIT 1;
            """,
            world
        )

        if not still_for_world:
            await self.db.execute(
                "DELETE FROM odtracker_configs_v2 WHERE world = $1;",
                world
            )

        still_any = await self.db.fetchval(
            "SELECT 1 FROM odtracker_enabled_tribes_v2 LIMIT 1;"
        )
        if not still_any:
            od_cog = self._get_tracker_cog("od")
            if od_cog is not None:
                try:
                    if hasattr(od_cog, "scan_od") and od_cog.scan_od.is_running():
                        od_cog.scan_od.cancel()
                    if hasattr(od_cog, "cleanup_odtracker") and od_cog.cleanup_odtracker.is_running():
                        od_cog.cleanup_odtracker.cancel()
                except Exception:
                    logger.exception("[ToggleTrackers] OD loops stop failed")

    # ---------------------- Slash command ---------------------- #

    @app_commands.command(
        name="toggle-trackers",
        description="Zet trackers aan/uit voor een wereld in dit kanaal."
    )
    async def toggle_trackers(self, interaction: discord.Interaction) -> None:
        if interaction.guild_id is None or interaction.channel_id is None:
            await interaction.response.send_message(
                "Deze command kan alleen in een serverkanaal gebruikt worden.",
                ephemeral=True
            )
            return

        embed = self._start_embed()
        view = TrackerSelectView(cog=self, user_id=interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# ---------------------- Shared "Back to start" ---------------------- #

class BackToStartButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Terug naar begin", style=discord.ButtonStyle.secondary, row=4)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view  # type: ignore
        cog = getattr(view, "cog", None)
        user_id = getattr(view, "user_id", None)

        if cog is None or user_id is None:
            await interaction.response.send_message("Er ging iets mis. Probeer opnieuw.", ephemeral=True)
            return

        embed = cog._start_embed()
        start_view = TrackerSelectView(cog=cog, user_id=user_id)
        await interaction.response.edit_message(embed=embed, view=start_view)


class BaseView(discord.ui.View):
    def __init__(self, cog: ToggleTrackers, user_id: int, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "Alleen de persoon die deze menu opende kan dit bedienen.",
                ephemeral=True
            )
            return False
        return True

    def add_back_button(self) -> None:
        self.add_item(BackToStartButton())


# ---------------------- Tracker select ---------------------- #

class TrackerSelectView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int):
        super().__init__(cog=cog, user_id=user_id)

        cfg = self.cog._get_tracker_configs()

        self.add_item(TrackerButton(tracker_id="academy", label=cfg["academy"]["label"], style=discord.ButtonStyle.primary))
        self.add_item(TrackerButton(tracker_id="wall", label=cfg["wall"]["label"], style=discord.ButtonStyle.primary))
        self.add_item(TrackerButton(tracker_id="tower", label=cfg["tower"]["label"], style=discord.ButtonStyle.primary))
        self.add_item(TrackerButton(tracker_id="conquer", label=cfg["conquer"]["label"], style=discord.ButtonStyle.primary))
        self.add_item(TrackerButton(tracker_id="od", label=cfg["od"]["label"], style=discord.ButtonStyle.primary))

        self.add_back_button()


class TrackerButton(discord.ui.Button):
    def __init__(self, tracker_id: str, label: str, style: discord.ButtonStyle):
        super().__init__(label=label, style=style)
        self.tracker_id = tracker_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view: TrackerSelectView = self.view  # type: ignore
        cog = view.cog
        tracker_cfg = cog._get_tracker_configs()[self.tracker_id]

        embed = cog._tracker_embed(self.tracker_id)
        next_view = OnOffChoiceView(cog=cog, user_id=interaction.user.id, tracker_id=self.tracker_id)
        await interaction.response.edit_message(embed=embed, view=next_view)


# ---------------------- On/Off choice (all trackers) ---------------------- #

class OnOffChoiceView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int, tracker_id: str):
        super().__init__(cog=cog, user_id=user_id)
        self.tracker_id = tracker_id

        self.add_item(ChooseOnButton())
        self.add_item(ChooseOffButton())
        self.add_back_button()


class ChooseOnButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Aan", style=discord.ButtonStyle.success)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: OnOffChoiceView = self.view  # type: ignore
        cog = view.cog
        tracker_cfg = cog._get_tracker_configs()[view.tracker_id]

        worlds = await cog._fetch_worlds_from_villagedata()
        if not worlds:
            embed = create_embed(title="Tracker aanpassen", description="Er zijn geen geactiveerde werelden gevonden.")
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"Voor welke wereld wil je de **{cog._fmt_tracker_label(view.tracker_id)}** tracker aan zetten?"
        )

        world_view = WorldSelectAfterChoiceView(
            cog=cog,
            user_id=interaction.user.id,
            tracker_id=view.tracker_id,
            mode="on",
            worlds=worlds
        )
        await interaction.response.edit_message(embed=embed, view=world_view)


class ChooseOffButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Uit", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: OnOffChoiceView = self.view  # type: ignore
        cog = view.cog
        tracker_cfg = cog._get_tracker_configs()[view.tracker_id]

        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        if view.tracker_id == "conquer":
            enabled = await cog._conquer_fetch_enabled_in_channel(guild_id, channel_id)
            if not enabled:
                embed = create_embed(
                    title="Tracker aanpassen",
                    description=f"Er staan op dit moment geen **{cog._fmt_tracker_label(view.tracker_id)}** aan in dit kanaal."
                )
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?"
            )
            disable_view = ConquerDisablePickView(cog=cog, user_id=interaction.user.id, entries=enabled)
            await interaction.response.edit_message(embed=embed, view=disable_view)
            return

        if view.tracker_id == "od":
            enabled = await cog._od_fetch_enabled_in_channel(guild_id, channel_id)
            if not enabled:
                embed = create_embed(
                    title="Tracker aanpassen",
                    description=f"Er staan op dit moment geen **{cog._fmt_tracker_label(view.tracker_id)}** aan in dit kanaal."
                )
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?"
            )
            disable_view = ODDisablePickView(cog=cog, user_id=interaction.user.id, entries=enabled)
            await interaction.response.edit_message(embed=embed, view=disable_view)
            return

        worlds = await cog._fetch_enabled_worlds_in_channel_simple(
            tracker_id=view.tracker_id,
            guild_id=guild_id,
            channel_id=channel_id
        )

        if not worlds:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Er staan op dit moment geen werelden aan voor **{cog._fmt_tracker_label(view.tracker_id)}** in dit kanaal."
            )
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"Voor welke wereld wil je de **{cog._fmt_tracker_label(view.tracker_id)}** tracker uit zetten?"
        )
        world_view = WorldSelectAfterChoiceView(
            cog=cog,
            user_id=interaction.user.id,
            tracker_id=view.tracker_id,
            mode="off",
            worlds=worlds
        )
        await interaction.response.edit_message(embed=embed, view=world_view)


# ---------------------- World pick view (reused) ---------------------- #

class WorldSelectAfterChoiceView(BaseView):
    def __init__(
        self,
        cog: ToggleTrackers,
        user_id: int,
        tracker_id: str,
        mode: str,
        worlds: List[str]
    ):
        super().__init__(cog=cog, user_id=user_id)
        self.tracker_id = tracker_id
        self.mode = mode  # "on" | "off"
        self.worlds = worlds
        self.page_size = 20
        self.page = 0

        self._render()
        self.add_back_button()

    def _get_page_worlds(self) -> List[str]:
        start = self.page * self.page_size
        end = start + self.page_size
        return self.worlds[start:end]

    def _max_page(self) -> int:
        if not self.worlds:
            return 0
        return max(0, (len(self.worlds) - 1) // self.page_size)

    def _render(self) -> None:
        to_remove = [c for c in self.children if isinstance(c, (WorldActionButton, PrevPageButton, NextPageButton))]
        for item in to_remove:
            self.remove_item(item)

        for w in self._get_page_worlds():
            self.add_item(WorldActionButton(world=w))

        if len(self.worlds) > self.page_size:
            self.add_item(PrevPageButton())
            self.add_item(NextPageButton())

    async def _update_message(self, interaction: discord.Interaction) -> None:
        tracker_cfg = self.cog._get_tracker_configs()[self.tracker_id]
        actie = "aan" if self.mode == "on" else "uit"
        max_page = self._max_page()

        if max_page > 0:
            description = (
                f"Voor welke wereld wil je de **{cog._fmt_tracker_label(view.tracker_id)}** tracker {actie} zetten?\n"
                f"Pagina {self.page + 1} van {max_page + 1}"
            )
        else:
            description = f"Voor welke wereld wil je de **{cog._fmt_tracker_label(view.tracker_id)}** tracker {actie} zetten?"

        embed = create_embed(title="Tracker aanpassen", description=description)
        self._render()
        await interaction.response.edit_message(embed=embed, view=self)


class WorldActionButton(discord.ui.Button):
    def __init__(self, world: str):
        super().__init__(label=world.upper(), style=discord.ButtonStyle.primary)
        self.world = world

    async def callback(self, interaction: discord.Interaction) -> None:
        view: WorldSelectAfterChoiceView = self.view  # type: ignore
        cog = view.cog
        tracker_cfg = cog._get_tracker_configs()[view.tracker_id]

        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        title = "Tracker aanpassen"

        # Conquer "aan" -> go to tribe dropdown
        if view.tracker_id == "conquer" and view.mode == "on":
            world_ok = await cog._conquer_world_is_enabled(self.world)
            if not world_ok:
                embed = create_embed(
                    title=title,
                    description=(
                        "Deze wereld is nog niet ingeschakeld. Contacteer de "
                        "[bot eigenaar](https://discord.com/users/284710799321202702)."
                    )
                )
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            tribes = await cog._conquer_fetch_tribes_for_world(self.world, search="")
            if not tribes:
                embed = create_embed(title=title, description="Ik kon geen stammen vinden voor deze wereld.")
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            embed = create_embed(title=title, description=f"Kies de stam die je wil aanzetten op `{self.world.upper()}`.")
            tribe_view = ConquerTribeSelectView(cog=cog, user_id=interaction.user.id, world=self.world, tribes=tribes)
            await interaction.response.edit_message(embed=embed, view=tribe_view)
            return

        # OD "aan" -> go to tribe dropdown (tags + alltribes)
        if view.tracker_id == "od" and view.mode == "on":
            embed = create_embed(
                title=title,
                description=f"Kies de stam die je wil aanzetten op `{self.world.upper()}` (of kies `alltribes`)."
            )
            tribe_view = ODTribeSelectView(cog=cog, user_id=interaction.user.id, world=self.world)
            await interaction.response.edit_message(embed=embed, view=tribe_view)
            return

        # Simple trackers
        if view.mode == "on":
            world_ok = await cog._world_is_enabled_villagedata(self.world)
            if not world_ok:
                embed = create_embed(
                    title=title,
                    description=(
                        "Deze wereld is nog niet ingeschakeld. Contacteer de "
                        "[bot eigenaar](https://discord.com/users/284710799321202702)."
                    )
                )
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            enabled = await cog._is_tracker_enabled_in_channel_simple(
                tracker_id=view.tracker_id,
                guild_id=guild_id,
                channel_id=channel_id,
                world=self.world
            )

            if enabled:
                embed = create_embed(
                    title=title,
                    description=f"**{cog._fmt_tracker_label(view.tracker_id).capitalize()}** tracker stond al aan voor wereld `{self.world.upper()}` in dit kanaal."
                )
                end_view = EndView(cog=cog, user_id=interaction.user.id)
                await interaction.response.edit_message(embed=embed, view=end_view)
                return

            await cog._enable_tracker_in_channel_simple(
                tracker_id=view.tracker_id,
                guild_id=guild_id,
                channel_id=channel_id,
                world=self.world
            )

            embed = create_embed(
                title=title,
                description=f"**{cog._fmt_tracker_label(view.tracker_id).capitalize()}** tracker succesvol aangezet voor wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        enabled = await cog._is_tracker_enabled_in_channel_simple(
            tracker_id=view.tracker_id,
            guild_id=guild_id,
            channel_id=channel_id,
            world=self.world
        )

        if not enabled:
            embed = create_embed(
                title=title,
                description=f"**{cog._fmt_tracker_label(view.tracker_id).capitalize()}** tracker stond al uit voor wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        await cog._disable_tracker_in_channel_simple(
            tracker_id=view.tracker_id,
            guild_id=guild_id,
            channel_id=channel_id,
            world=self.world
        )

        embed = create_embed(
            title=title,
            description=f"**{cog._fmt_tracker_label(view.tracker_id).capitalize()}** tracker succesvol uitgezet voor wereld `{self.world.upper()}` in dit kanaal."
        )
        end_view = EndView(cog=cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


# ---------------------- Conquer tribe dropdown (with search) ---------------------- #

class ConquerTribeSearchModal(discord.ui.Modal):
    def __init__(self, parent_view: "ConquerTribeSelectView"):
        super().__init__(title="Stam zoeken")
        self.parent_view = parent_view

        self.query = discord.ui.TextInput(
            label="Zoekterm",
            placeholder="Bijv. tag of deel van de tag",
            required=False,
            max_length=50
        )
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.parent_view.apply_search(interaction, str(self.query.value or "").strip())


class ConquerTribeSelect(discord.ui.Select):
    def __init__(self, parent_view: "ConquerTribeSelectView", options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Kies een stam",
            min_values=1,
            max_values=1,
            options=options
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.parent_view.handle_select(interaction, self.values[0])


class ConquerTribeSelectView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int, world: str, tribes: List[Tuple[int, str]]):
        super().__init__(cog=cog, user_id=user_id)
        self.world = world

        self._set_options(tribes)
        self.add_item(ConquerSearchButton())
        self.add_item(ConquerCancelButton())
        self.add_back_button()

    def _set_options(self, tribes: List[Tuple[int, str]]) -> None:
        options: List[discord.SelectOption] = []
        for tribe_id, tag in tribes[:25]:
            options.append(discord.SelectOption(label=tag, value=str(tribe_id)))

        for item in list(self.children):
            if isinstance(item, ConquerTribeSelect):
                self.remove_item(item)

        self.add_item(ConquerTribeSelect(self, options))

    async def apply_search(self, interaction: discord.Interaction, search: str) -> None:
        tribes = await self.cog._conquer_fetch_tribes_for_world(self.world, search=search)

        if search:
            description = f"Kies de stam die je wil aanzetten op `{self.world.upper()}`.\nZoekterm: `{search}`"
        else:
            description = f"Kies de stam die je wil aanzetten op `{self.world.upper()}`."
        embed = create_embed(title="Tracker aanpassen", description=description)

        if not tribes:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Geen stammen gevonden voor `{self.world.upper()}` met zoekterm `{search}`."
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return

        self._set_options(tribes)
        await interaction.response.edit_message(embed=embed, view=self)

    async def handle_select(self, interaction: discord.Interaction, tribe_id_value: str) -> None:
        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        try:
            tribe_id = int(tribe_id_value)
        except ValueError:
            await interaction.response.send_message("Ongeldige stam selectie.", ephemeral=True)
            return

        tag = await self.cog.db.fetchval(
            """
            SELECT tag
            FROM ally_data
            WHERE world = $1 AND tribe_id = $2
            LIMIT 1;
            """,
            self.world, tribe_id
        )
        if not tag:
            embed = create_embed(title="Tracker aanpassen", description="Deze stam bestaat niet (meer) op deze wereld.")
            end_view = EndView(cog=self.cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        already = await self.cog._conquer_is_enabled(guild_id, channel_id, self.world, tribe_id)
        if already:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Veroveringen tracker stond al aan voor stam `{tag}` op wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=self.cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        await self.cog._conquer_enable(guild_id, channel_id, self.world, tribe_id)

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"Veroveringen tracker succesvol aangezet voor stam `{tag}` op wereld `{self.world.upper()}` in dit kanaal."
        )
        end_view = EndView(cog=self.cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


class ConquerSearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Zoeken", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ConquerTribeSelectView = self.view  # type: ignore
        await interaction.response.send_modal(ConquerTribeSearchModal(view))


class ConquerCancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Annuleren", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ConquerTribeSelectView = self.view  # type: ignore
        embed = create_embed(title="Tracker aanpassen", description="Actie geannuleerd.")
        end_view = EndView(cog=view.cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


# ---------------------- OD tribe dropdown (with search) ---------------------- #

class ODTribeSearchModal(discord.ui.Modal):
    def __init__(self, parent_view: "ODTribeSelectView"):
        super().__init__(title="Stam zoeken")
        self.parent_view = parent_view

        self.query = discord.ui.TextInput(
            label="Zoekterm",
            placeholder="Bijv. tag of deel van de tag",
            required=False,
            max_length=50
        )
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.parent_view.apply_search(interaction, str(self.query.value or "").strip())


class ODTribeSelect(discord.ui.Select):
    def __init__(self, parent_view: "ODTribeSelectView", options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Kies een stam",
            min_values=1,
            max_values=1,
            options=options
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.parent_view.handle_select(interaction, self.values[0])


class ODTribeSelectView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int, world: str):
        super().__init__(cog=cog, user_id=user_id)
        self.world = world

        self.add_item(ODSearchButton())
        self.add_item(ODCancelButton())
        self.add_back_button()

        self._set_options(tags=["alltribes"])

    def _set_options(self, tags: List[str]) -> None:
        options: List[discord.SelectOption] = []
        for tag in tags[:25]:
            label = tag
            options.append(discord.SelectOption(label=label, value=tag))

        for item in list(self.children):
            if isinstance(item, ODTribeSelect):
                self.remove_item(item)

        self.add_item(ODTribeSelect(self, options))

    async def apply_search(self, interaction: discord.Interaction, search: str) -> None:
        base = ["alltribes"]

        tags = await self.cog._od_fetch_tags_for_world(self.world, search=search)
        combined = base + [t for t in tags if t.lower() != "alltribes"]

        if search:
            description = f"Kies de stam die je wil aanzetten op `{self.world.upper()}` (of kies `alltribes`).\nZoekterm: `{search}`"
        else:
            description = f"Kies de stam die je wil aanzetten op `{self.world.upper()}` (of kies `alltribes`)."
        embed = create_embed(title="Tracker aanpassen", description=description)

        if len(combined) == 1 and combined[0] == "alltribes" and search:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Geen stammen gevonden voor `{self.world.upper()}` met zoekterm `{search}`."
            )
            self._set_options(tags=["alltribes"])
            await interaction.response.edit_message(embed=embed, view=self)
            return

        self._set_options(tags=combined[:25])
        await interaction.response.edit_message(embed=embed, view=self)

    async def handle_select(self, interaction: discord.Interaction, tribe_tag: str) -> None:
        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        tag_value = (tribe_tag or "").strip()
        if not tag_value:
            await interaction.response.send_message("Ongeldige stam selectie.", ephemeral=True)
            return

        already = await self.cog._od_is_enabled(guild_id, channel_id, self.world, tag_value)
        if already:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"OD-tracker stond al aan voor stam `{tag_value}` op wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=self.cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        await self.cog._od_enable(guild_id, channel_id, self.world, tag_value)

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"OD-tracker succesvol aangezet voor stam `{tag_value}` op wereld `{self.world.upper()}` in dit kanaal."
        )
        end_view = EndView(cog=self.cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


class ODSearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Zoeken", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ODTribeSelectView = self.view  # type: ignore
        await interaction.response.send_modal(ODTribeSearchModal(view))


class ODCancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Annuleren", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ODTribeSelectView = self.view  # type: ignore
        embed = create_embed(title="Tracker aanpassen", description="Actie geannuleerd.")
        end_view = EndView(cog=view.cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


# ---------------------- Conquer disable pick (world|tag buttons) ---------------------- #

class ConquerDisablePickView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int, entries: List[Tuple[str, int, str]]):
        super().__init__(cog=cog, user_id=user_id)
        self.entries = entries
        self.page_size = 20
        self.page = 0

        self._render()
        self.add_back_button()

    def _max_page(self) -> int:
        if not self.entries:
            return 0
        return max(0, (len(self.entries) - 1) // self.page_size)

    def _page_entries(self) -> List[Tuple[str, int, str]]:
        start = self.page * self.page_size
        end = start + self.page_size
        return self.entries[start:end]

    def _render(self) -> None:
        to_remove = [c for c in self.children if isinstance(c, (ConquerDisableEntryButton, ConquerDisablePrevPageButton, ConquerDisableNextPageButton))]
        for item in to_remove:
            self.remove_item(item)

        for (world, tribe_id, tag) in self._page_entries():
            self.add_item(ConquerDisableEntryButton(world=world, tribe_id=tribe_id, tag=tag))

        if len(self.entries) > self.page_size:
            self.add_item(ConquerDisablePrevPageButton())
            self.add_item(ConquerDisableNextPageButton())

    async def _update(self, interaction: discord.Interaction) -> None:
        tracker_cfg = self.cog._get_tracker_configs()["conquer"]
        max_page = self._max_page()
        if max_page > 0:
            description = f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?\nPagina {self.page + 1} van {max_page + 1}"
        else:
            description = f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?"
        embed = create_embed(title="Tracker aanpassen", description=description)

        self._render()
        await interaction.response.edit_message(embed=embed, view=self)


class ConquerDisableEntryButton(discord.ui.Button):
    def __init__(self, world: str, tribe_id: int, tag: str):
        label = f"{world.upper()} | {tag}"
        if len(label) > 80:
            label = label[:77] + "..."
        super().__init__(label=label, style=discord.ButtonStyle.danger)
        self.world = world
        self.tribe_id = tribe_id
        self.tag = tag

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ConquerDisablePickView = self.view  # type: ignore
        cog = view.cog

        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        exists = await cog._conquer_is_enabled(guild_id, channel_id, self.world, self.tribe_id)
        if not exists:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"Veroveringen tracker stond al uit voor stam `{self.tag}` op wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        await cog._conquer_disable(guild_id, channel_id, self.world, self.tribe_id)

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"Veroveringen tracker succesvol uitgezet voor stam `{self.tag}` op wereld `{self.world.upper()}` in dit kanaal."
        )
        end_view = EndView(cog=cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


class ConquerDisablePrevPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Vorige", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ConquerDisablePickView = self.view  # type: ignore
        if view.page > 0:
            view.page -= 1
        await view._update(interaction)


class ConquerDisableNextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Volgende", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ConquerDisablePickView = self.view  # type: ignore
        max_page = view._max_page()
        if view.page < max_page:
            view.page += 1
        await view._update(interaction)


# ---------------------- OD disable pick (world|tribe_tag buttons) ---------------------- #

class ODDisablePickView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int, entries: List[Tuple[str, str]]):
        super().__init__(cog=cog, user_id=user_id)
        self.entries = entries
        self.page_size = 20
        self.page = 0

        self._render()
        self.add_back_button()

    def _max_page(self) -> int:
        if not self.entries:
            return 0
        return max(0, (len(self.entries) - 1) // self.page_size)

    def _page_entries(self) -> List[Tuple[str, str]]:
        start = self.page * self.page_size
        end = start + self.page_size
        return self.entries[start:end]

    def _render(self) -> None:
        to_remove = [c for c in self.children if isinstance(c, (ODDisableEntryButton, ODDisablePrevPageButton, ODDisableNextPageButton))]
        for item in to_remove:
            self.remove_item(item)

        for (world, tribe_tag) in self._page_entries():
            self.add_item(ODDisableEntryButton(world=world, tribe_tag=tribe_tag))

        if len(self.entries) > self.page_size:
            self.add_item(ODDisablePrevPageButton())
            self.add_item(ODDisableNextPageButton())

    async def _update(self, interaction: discord.Interaction) -> None:
        tracker_cfg = self.cog._get_tracker_configs()["od"]
        max_page = self._max_page()
        if max_page > 0:
            description = f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?\nPagina {self.page + 1} van {max_page + 1}"
        else:
            description = f"Welke **{cog._fmt_tracker_label(view.tracker_id)}** wil je uit zetten?"
        embed = create_embed(title="Tracker aanpassen", description=description)

        self._render()
        await interaction.response.edit_message(embed=embed, view=self)


class ODDisableEntryButton(discord.ui.Button):
    def __init__(self, world: str, tribe_tag: str):
        label = f"{world.upper()} | {tribe_tag}"
        if len(label) > 80:
            label = label[:77] + "..."
        super().__init__(label=label, style=discord.ButtonStyle.danger)
        self.world = world
        self.tribe_tag = tribe_tag

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ODDisablePickView = self.view  # type: ignore
        cog = view.cog

        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        if guild_id is None or channel_id is None:
            await interaction.response.send_message("Deze actie kan alleen in een serverkanaal gebruikt worden.", ephemeral=True)
            return

        exists = await cog._od_is_enabled(guild_id, channel_id, self.world, self.tribe_tag)
        if not exists:
            embed = create_embed(
                title="Tracker aanpassen",
                description=f"OD-tracker stond al uit voor stam `{self.tribe_tag}` op wereld `{self.world.upper()}` in dit kanaal."
            )
            end_view = EndView(cog=cog, user_id=interaction.user.id)
            await interaction.response.edit_message(embed=embed, view=end_view)
            return

        await cog._od_disable(guild_id, channel_id, self.world, self.tribe_tag)

        embed = create_embed(
            title="Tracker aanpassen",
            description=f"OD-tracker succesvol uitgezet voor stam `{self.tribe_tag}` op wereld `{self.world.upper()}` in dit kanaal."
        )
        end_view = EndView(cog=cog, user_id=interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=end_view)


class ODDisablePrevPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Vorige", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ODDisablePickView = self.view  # type: ignore
        if view.page > 0:
            view.page -= 1
        await view._update(interaction)


class ODDisableNextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Volgende", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: ODDisablePickView = self.view  # type: ignore
        max_page = view._max_page()
        if view.page < max_page:
            view.page += 1
        await view._update(interaction)


# ---------------------- Pagination buttons (shared) ---------------------- #

class PrevPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Vorige", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view  # type: ignore
        if hasattr(view, "page") and hasattr(view, "_update_message"):
            if view.page > 0:
                view.page -= 1
            await view._update_message(interaction)


class NextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Volgende", style=discord.ButtonStyle.primary, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view  # type: ignore
        if hasattr(view, "page") and hasattr(view, "_max_page") and hasattr(view, "_update_message"):
            max_page = view._max_page()
            if view.page < max_page:
                view.page += 1
            await view._update_message(interaction)


# ---------------------- End view (always with back button) ---------------------- #

class EndView(BaseView):
    def __init__(self, cog: ToggleTrackers, user_id: int):
        super().__init__(cog=cog, user_id=user_id)
        self.add_back_button()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ToggleTrackers(bot))
