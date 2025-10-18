import pandas as pd
import matplotlib.pyplot as plt

# ---------------------- 0️⃣ Đọc dữ liệu ----------------------
file_path = "/Users/gianghi/BigDataProjects/BigData/Charts/spotify.csv"
try:
    df = pd.read_csv(file_path, on_bad_lines='skip')
except Exception as e:
    print(f"LỖI KHI ĐỌC FILE: {e}")
    exit()

print(f"✅ ĐỌC THÀNH CÔNG {len(df)} DÒNG!")
print("Columns:", df.columns.tolist())
print(df.head())

date_col, rank_col, artist_col = 'date', 'rank', 'artists'

# ---------------------- 1️⃣ Biểu đồ đường xếp hạng trung bình ----------------------
df['date_only'] = df[date_col].str[:10]
df[rank_col] = pd.to_numeric(df[rank_col], errors='coerce')

avg_rank = df.groupby('date_only')[rank_col].mean().sort_index()

plt.figure(figsize=(12,6))
plt.plot(avg_rank.index, avg_rank.values, color='blue', linewidth=2, marker='o', markersize=4, label='Rank trung bình')

# Thêm vùng sáng dưới đường
plt.fill_between(avg_rank.index, avg_rank.values, avg_rank.values.max(), color='blue', alpha=0.1)

plt.title('XẾP HẠNG TRUNG BÌNH SPOTIFY THEO NGÀY', fontsize=16, fontweight='bold')
plt.xlabel('Ngày', fontsize=12)
plt.ylabel('Rank trung bình', fontsize=12)
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()

# ---------------------- 2️⃣ Biểu đồ cột ngang top 10 nghệ sĩ ----------------------
artist_list = df[artist_col].dropna().str.split(',', expand=True).stack().str.strip()
top_artists = artist_list.value_counts().head(10)

plt.figure(figsize=(12,6))

# Gradient giả lập bằng nhiều màu khác nhau
colors = plt.cm.viridis([i/len(top_artists) for i in range(len(top_artists))])

plt.barh(top_artists.index[::-1], top_artists.values[::-1], color=colors, alpha=0.9)

# Thêm label bên ngoài cột
for i, val in enumerate(top_artists.values[::-1]):
    plt.text(val + 0.2, i, f'{val}', va='center', fontsize=10)

plt.title('TOP 10 NGHỆ SĨ - SỐ BÀI HÁT TRONG BẢNG XẾP HẠNG', fontsize=16, fontweight='bold')
plt.xlabel('Số lượng bài hát', fontsize=12)
plt.ylabel('Nghệ sĩ', fontsize=12)
plt.grid(True, axis='x', alpha=0.3)
plt.tight_layout()
plt.show()

print("🎉 XONG! 2 BIỂU ĐỒ XỊN XỊN CHỈ DÙNG MATPLOTLIB! ✨")
