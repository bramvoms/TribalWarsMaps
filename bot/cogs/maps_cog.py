import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
from typing import List, Literal
from io import BytesIO 
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MapCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.worlds = []

    async def fetch_worlds(self):
        """Fetch available worlds from the API and populate self.bot.worlds."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://dkspeed2.jrsoft.tech/api/worlds') as response:
                    if response.status == 200:
                        self.bot.worlds = await response.json()
                    else:
                        logger.error(f"Failed to fetch worlds. Status: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching worlds: {e}")

    async def cog_load(self):
        """Run tasks when the cog is loaded."""
        await self.fetch_worlds()

    @app_commands.command(name="map", description="Generate a map for a specified world and type.")
    @app_commands.describe(
        world="The world name to get a map for.",
        type="The type of map view."
    )
    async def map_command(
        self,
        interaction: discord.Interaction,
        world: str,
        type: Literal[
            "top-15-ally-conquers",
            "top-15-ally",
            "top-15-player",
            "top-15-player-conquers"
        ]
    ):
        await interaction.response.defer()

        if world not in self.bot.worlds:
            await interaction.followup.send(f"Invalid world. Available worlds: {', '.join(self.bot.worlds)}", ephemeral=True)
            return

        api_url = f"https://dkspeed2.jrsoft.tech/map2/{world}/{type}"
        logger.info(f"Calling API URL: {api_url}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200 and "image/png" in response.headers.get("Content-Type", ""):
                        image_data = await response.read()
                        image_file = discord.File(BytesIO(image_data), filename="map.png")
                        await interaction.followup.send(file=image_file)
                    else:
                        await interaction.followup.send("Failed to get map or unexpected response format.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in map_command: {e}")
            await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)
            
    @app_commands.command(name="map-custom", description="Generate a map for specified tribes or players.")
    @app_commands.describe(
        world="The world name to get a map for.",
        type="The type of map view.",
        names="Comma-separated list of tribe/player names."
    )
    async def map_custom_command(
        self,
        interaction: discord.Interaction,
        world: str,
        type: Literal["ally-conquers", "ally", "player-conquers", "player"],
        names: str
    ):
        await interaction.response.defer()

        if world not in self.bot.worlds:
            await interaction.followup.send(f"Invalid world. Available worlds: {', '.join(self.bot.worlds)}", ephemeral=True)
            return

        # Format names as query parameters
        param_key = "allies[]" if "ally" in type else "players[]"
        query_params = "&".join([f"{param_key}={name.strip()}" for name in names.split(",")])

        api_url = f"https://dkspeed2.jrsoft.tech/map2/{world}/{type}?{query_params}"
        logger.info(f"Calling API URL: {api_url}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200 and "image/png" in response.headers.get("Content-Type", ""):
                        image_data = await response.read()
                        image_file = discord.File(BytesIO(image_data), filename="map.png")
                        await interaction.followup.send(file=image_file)
                    else:
                        await interaction.followup.send("Failed to get map or unexpected response format.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in map_custom_command: {e}")
            await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)

    @map_command.autocomplete("world")
    @map_custom_command.autocomplete("world")
    async def world_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """Autocomplete for the 'world' parameter."""
        try:
            logger.debug(f"Autocomplete triggered with input: '{current}'")
            
            if not self.bot.worlds:
                logger.warning("Autocomplete called but no worlds are available.")
                await self.fetch_worlds()

            filtered_worlds = [
                app_commands.Choice(name=world, value=world)
                for world in self.bot.worlds if current.lower() in world.lower()
            ][:25]
            
            return filtered_worlds
        except Exception as e:
            logger.error(f"Error in world_autocomplete: {e}")
            return []

async def setup(bot):
    await bot.add_cog(MapCog(bot))
