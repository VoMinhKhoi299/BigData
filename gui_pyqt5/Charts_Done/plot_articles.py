import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

file_path = "/Users/gianghi/BigDataProjects/BigData/Charts/articles.csv"
try:
    # Định nghĩa tên cột dựa trên dữ liệu mẫu
    column_names = ['title', 'date', 'category', 'summary', 'source']
    df = pd.read_csv(file_path, names=column_names, header=None)
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {file_path}.")
    exit(1)

print("Columns in articles.csv:", df.columns.tolist())

if 'date' not in df.columns:
    print("Lỗi: Không tìm thấy cột ngày ('date').")
    exit(1)

if 'category' not in df.columns:
    print("Lỗi: Không tìm thấy cột danh mục ('category').")
    exit(1)

if 'summary' not in df.columns:
    print("Lỗi: Không tìm thấy cột tóm tắt ('summary').")
    exit(1)

try:
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
except Exception as e:
    print(f"Lỗi khi chuyển đổi cột ngày 'date': {e}")
    exit(1)

# ---------------------- 1️ Biểu đồ đường - Số bài viết theo ngày ----------------------
articles_per_day = df.groupby(df['date'].dt.date).size().reset_index(name='count')
plt.figure(figsize=(12, 6))
plt.plot(articles_per_day['date'], articles_per_day['count'], color='blue', label='Số bài viết')
plt.title('Số bài viết VNExpress theo ngày', fontsize=14)
plt.xlabel('Ngày')
plt.ylabel('Số bài viết')
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()
plt.show()

# ---------------------- 2️ Biểu đồ cột - Số bài viết theo danh mục ----------------------
category_counts = df['category'].value_counts().head(10)  # Lấy top 10 danh mục
plt.figure(figsize=(12, 6))
category_counts.plot(kind='bar', color='orange', alpha=0.7)
plt.title('Số bài viết theo danh mục (Top 10)', fontsize=14)
plt.xlabel('Danh mục')
plt.ylabel('Số bài viết')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()

# ---------------------- 3️ Biểu đồ phân tán - Độ dài tóm tắt ----------------------
df['summary_length'] = df['summary'].apply(lambda x: len(str(x).split()) if pd.notnull(x) else 0)
plt.figure(figsize=(8, 6))
plt.scatter(df['date'], df['summary_length'], c='purple', alpha=0.6)
plt.title('Phân bố độ dài tóm tắt bài viết theo ngày', fontsize=14)
plt.xlabel('Ngày')
plt.ylabel('Độ dài tóm tắt (số từ)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()