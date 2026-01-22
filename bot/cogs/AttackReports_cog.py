import asyncio
import io
import os
import re
import subprocess
import time
import discord
from discord.ext import commands
from playwright.async_api import async_playwright

async def ensure_chromium_installed():
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/playwright")
        marker_path = "/tmp/chromium_installed"

        if os.path.exists(marker_path):
            return

        try:
            print("Chromium niet gevonden, installatie starten...")
            process = await asyncio.create_subprocess_exec(
                "playwright", "install", "chromium"
            )
            await process.communicate()

            if process.returncode == 0:
                with open(marker_path, "w") as f:
                    f.write("ok")
                print("Chromium succesvol geïnstalleerd.")
            else:
                print(f"playwright install chromium faalde met code {process.returncode}")
        except Exception as e:
            print(f"Fout bij installeren van Chromium: {e}")

class ReportScreenshotCog(commands.Cog):
        def __init__(self, bot):
            self.bot = bot
            self._last_screenshot_per_channel: dict[int, float] = {}

        @commands.Cog.listener()
        async def on_message(self, message: discord.Message):
            if message.author.bot:
                return

            if not message.guild:
                return

            urls = self._extract_report_urls(message.content)
            if not urls:
                return

            now = time.monotonic()
            last = self._last_screenshot_per_channel.get(message.channel.id, 0)
            if now - last < 10:
                return
            self._last_screenshot_per_channel[message.channel.id] = now

            url = urls[0]

            screenshot_file = await self._create_report_screenshot(url)
            if screenshot_file is None:
                return

            await message.channel.send(
                content="",
                file=screenshot_file,
                reference=message,
                mention_author=False,
            )

        def _extract_report_urls(self, content: str) -> list[str]:
            pattern = re.compile(
                r"https://nl[a-zA-Z0-9]+\.tribalwars\.nl/public_report/(?=[0-9a-fA-F]*\d)[0-9a-fA-F]+"
            )
            return pattern.findall(content)

        async def _create_report_screenshot(self, url: str) -> discord.File | None:
                    try:
                        await ensure_chromium_installed()
        
                        async with async_playwright() as p:
                            browser = await p.chromium.launch(
                                headless=True,
                                args=["--no-sandbox", "--disable-dev-shm-usage"],
                            )
                            page = await browser.new_page(device_scale_factor=2)
                            await page.goto(url, wait_until="networkidle")
        
                            screenshot_bytes: bytes | None = None
                            try:
                                await page.wait_for_selector("h1 + table.vis", timeout=5000)
                                element = await page.query_selector("h1 + table.vis")
        
                                if element is None:
                                    element = await page.query_selector("table.vis[width='450']")
        
                                if element is not None:
                                    screenshot_bytes = await element.screenshot(type="png")
                            except Exception:
                                screenshot_bytes = None
        
                            if screenshot_bytes is None:
                                screenshot_bytes = await page.screenshot(
                                    full_page=True,
                                    type="png",
                                )
        
                            await browser.close()
        
                        buffer = io.BytesIO(screenshot_bytes)
                        buffer.seek(0)
                        return discord.File(buffer, filename="aanvalsrapport.png")
        
                    except Exception as e:
                        print(f"Fout bij maken aanvalsrapport screenshot: {e}")
                        return None

async def setup(bot):
        await bot.add_cog(ReportScreenshotCog(bot))
