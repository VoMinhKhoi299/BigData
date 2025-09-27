# Big Data Project

## 2️⃣ Quy trình làm việc

### a. Cấu trúc kho Git
```bash
bigdata-project/
    ├─ data_clean/ # Mã thu thập & làm sạch dữ liệu
    ├─ mapreduce_jobs/ # Mã MapReduce
    ├─ hbase_mysql/ # Script MySQL/HBase + Sqoop
    ├─ gui_pyqt5/ # Ứng dụng GUI PyQt5
    └─ docs/ # Tài liệu
```
- **Branch cho từng thành viên**  
  - `data` – xử lý dữ liệu thô  
  - `mapreduce` – viết job MapReduce  
  - `db` – quản lý MySQL/HBase + Sqoop  
  - `gui-crud` – giao diện CRUD  
  - `gui-chart` – biểu đồ & thống kê  

> Mỗi thành viên phát triển trên branch riêng và **merge vào `main` sau khi review**.

---
### b. Dòng công việc chuẩn

1. **Thu thập & làm sạch dữ liệu**
    - Thành viên A crawl dữ liệu và làm sạch
    - Push file `clean_data.csv` vào repo

2. **Đưa dữ liệu lên HDFS (máy chính)**
    - push code → Máy chính compile & run:
    - Lệnh:
        hdfs dfs -put clean_data.csv /input/project/

3. **Viết & chạy MapReduce**
    - Thành viên B push code
    - Máy chính compile & chạy:
        hadoop jar job.jar input output

4. **Kết quả → Database**
    - Thành viên C push script MySQL/HBase + Sqoop
    - Máy chính thực thi:
        mysql < create_mysql.sql
        hbase shell create_hbase.sql
        bash sqoop_export.sh

5. **Phát triển GUI**
    - Thành viên D & E phát triển PyQt5, test với local DB
    - [*Dự kiến] Khi hoàn thiện, cập nhật cấu hình:
        host = "IP_MAY_CHINH"
        port = 3306          # MySQL
        phoenix_url = "http://IP_MAY_CHINH:8765/"
    - Push code → Máy chính pull và chạy GUI thực tế

Phoenix Query Server: 8765