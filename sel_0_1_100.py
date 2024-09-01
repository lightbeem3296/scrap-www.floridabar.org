import os
from pathlib import Path

CUR_DIR = Path(__file__).parent

fname = os.path.basename(__file__)
words = os.path.splitext(fname)[0].split("_")

index, page_min, page_max = 0, 1, None
if len(words) >= 2:
    index = int(words[1].strip())
if len(words) >= 3:
    page_min = int(words[2].strip())
if len(words) >= 4:
    page_max = int(words[3].strip())

scraper_path = CUR_DIR / "scraper.py"

if page_max is None:
    cmdline = f"{scraper_path.absolute()} --index={index} --page_min={page_min}"
else:
    cmdline = f"{scraper_path.absolute()} --index={index} --page_min={page_min} --page_max={page_max}"

os.system(cmdline)
