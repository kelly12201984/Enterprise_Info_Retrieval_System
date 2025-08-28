from PIL import Image
from pathlib import Path

src = Path(r"P:\Chris\TankFinder\assets\tankfinder.png")
dst = Path(r"P:\Chris\TankFinder\assets\tankfinder.ico")

img = Image.open(src).convert("RGBA")
img.save(dst, sizes=[(16,16),(24,24),(32,32),(48,48),(64,64),(128,128),(256,256)])
print("Wrote", dst)
