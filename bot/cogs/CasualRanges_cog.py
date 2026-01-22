import discord
from discord import app_commands
from discord.ext import commands
import asyncpg
import os

class CasualRangesCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db

    async def get_database_connection(self):
        """Connect to the PostgreSQL database."""
        return self.db

    async def fetch_worlds(self):
        """Fetch active worlds from the playerdata_worlds table."""
        conn = await self.get_database_connection()
        query = "SELECT world FROM playerdata_worlds"
        return await conn.fetch(query)

    async def fetch_accounts(self, world: str):
        """Fetch active accounts on the selected world."""
        conn = await self.get_database_connection()
        query = "SELECT name FROM player_data WHERE world = $1"
        return await conn.fetch(query, world)

    async def fetch_players_in_range(self, world: str, account_name: str, percentage: int):
        """Fetch players within the given points range."""
        conn = await self.get_database_connection()
        player_query = """
            SELECT points FROM player_data 
            WHERE world = $1 AND name = $2
        """
        player = await conn.fetchrow(player_query, world, account_name)
        if not player:
            return None, None, None, None
            
        points = player["points"]
        min_points = round(points / (1 + (percentage / 100)))
        max_points = round(points * (1 + (percentage / 100)))

        range_query = """
            SELECT name, points FROM player_data 
            WHERE world = $1 AND points BETWEEN $2 AND $3 AND name != $4
            ORDER BY points DESC
        """
        players_in_range = await conn.fetch(range_query, world, min_points, max_points, account_name)
        return points, min_points, max_points, players_in_range

    @app_commands.command(name="casualrange", description="Vind spelers binnen een bepaalde range van een andere speler.")
    @app_commands.describe(
        wereld="Selecteer de wereld",
        account="Selecteer het account",
        range="Selecteer de range"
    )
    async def casualrange_command(self, interaction: discord.Interaction, wereld: str, account: str, range: str):
        """Handle the range command."""
        percentage = int(range.strip('%'))
        points, min_points, max_points, players = await self.fetch_players_in_range(wereld, account, percentage)

        if players is None:
            await interaction.response.send_message(f"Account `{account}` niet gevonden op `{wereld}`.", ephemeral=True)
            return
        
        if not players:
            player_list = "*Geen spelers in range van deze speler.*"
        else:
            player_list = "\n".join([f"**{p['name']}** - {int(p['points']):,}".replace(",", ".") + " punten" for p in players])

        embed = discord.Embed(
            title=f"Spelers binnen {range} van {account}",
            description=(
                f"**{account}** heeft **{int(points):,}** punten\n"
                f"**Minimale range:** {int(min_points):,} punten\n"
                f"**Maximale range:** {int(max_points):,} punten\n\n"
                f"**Spelers in range:**\n{player_list}"
            ).replace(",", "."),
            color=discord.Color.from_rgb(221, 205, 165)
        )
        await interaction.response.send_message(embed=embed)

    @casualrange_command.autocomplete("wereld")
    async def wereld_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for world selection."""
        worlds = await self.fetch_worlds()
        return [
            app_commands.Choice(name=w["world"], value=w["world"]) 
            for w in worlds if current.lower() in w["world"].lower()
        ][:25]

    @casualrange_command.autocomplete("account")
    async def account_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for account selection based on the chosen world."""
        wereld = interaction.namespace.wereld
        if not wereld:
            return []

        accounts = await self.fetch_accounts(wereld)
        return [
            app_commands.Choice(name=a["name"], value=a["name"]) 
            for a in accounts if current.lower() in a["name"].lower()
        ][:25]

    @casualrange_command.autocomplete("range")
    async def range_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for range selection."""
        ranges = ["20%", "40%", "70%", "100%", "150%", "200%", "300%"]
        return [app_commands.Choice(name=r, value=r) for r in ranges if current in r]

async def setup(bot):
    await bot.add_cog(CasualRangesCog(bot))
