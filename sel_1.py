import os
from pathlib import Path

CUR_DIR = Path(__file__).parent

fname = os.path.basename(__file__)
words = os.path.splitext(fname)[0].split("_")
index = int(words[1].strip())

scraper_path = CUR_DIR / "scraper.py"
cmdline = f"{scraper_path.absolute()} --index={index}"

os.system(cmdline)
