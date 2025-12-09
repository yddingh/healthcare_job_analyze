import pandas as pd
import sqlite3
from pathlib import Path
from tkinter import Tk, filedialog

# --- Step 1: 选择 CSV 文件 ---
root = Tk()
root.withdraw()
root.update()

csv_file = filedialog.askopenfilename(
    title="请选择要导入的 CSV 文件",
    filetypes=[("CSV 文件", "*.csv"), ("所有文件", "*.*")]
)

if not csv_file:
    print("未选择 CSV 文件，程序结束。")
    root.destroy()
    exit()

CSV_PATH = Path(csv_file)
print(f"已选择 CSV 文件: {CSV_PATH}")

# --- Step 2: 选择保存数据库的文件夹 ---
db_folder = filedialog.askdirectory(title="请选择要保存数据库的文件夹")

root.destroy()

if not db_folder:
    print("未选择保存路径，程序结束。")
    exit()

DB_PATH = Path(db_folder) / f"{CSV_PATH.stem}.db"
print(f"数据库将保存为: {DB_PATH}")

# --- Step 3: 读取 CSV ---
print("正在读取 CSV 文件...")
df = pd.read_csv(CSV_PATH)
print(f"CSV 读取完成: {len(df)} 行, {len(df.columns)} 列")

# --- Step 4: 写入 SQLite ---
print("正在写入 SQLite 数据库...")
conn = sqlite3.connect(DB_PATH)
df.to_sql("stg_jobs", conn, if_exists="replace", index=False)
conn.close()

print(f"数据已写入数据库: {DB_PATH}\n表名: stg_jobs")
