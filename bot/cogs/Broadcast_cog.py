from discord.ext import commands
import discord
from main import create_embed

class BroadcastCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="broadcast")
    @commands.is_owner()
    async def broadcast(self, ctx, *, message: str):
        title = "UPDATE"
        embed = create_embed(title=title, description=message)
        embed.set_thumbnail(url="https://i.imgur.com/7kjmru2.png")

        successful_sends = 0

        for guild in self.bot.guilds:
            channel = guild.system_channel
            if channel:
                try:
                    await channel.send(embed=embed)
                    successful_sends += 1
                except discord.Forbidden:
                    pass

        await ctx.send(f"Broadcast verzonden naar {successful_sends} servers.")

async def setup(bot):
    await bot.add_cog(BroadcastCog(bot))
