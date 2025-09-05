import json
import sys

data = sys.stdin.read().strip()
lines = data.split("\n")

# 找到JSON数据行（通常是最后一行或以[开头的行）
json_line = None
for line in lines:
    line = line.strip()
    if line.startswith("[") or line.startswith("{"):
        json_line = line
        break

if json_line:
    watchlist = json.loads(json_line)
    print(f"get {watchlist}")
else:
    print("No JSON data found")
