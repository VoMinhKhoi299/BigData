# BigData
-- 2.Quy trình làm việc
- a. Kho Git
bigdata-project/
 ├─ data_clean/
 ├─ mapreduce_jobs/
 ├─ hbase_mysql/
 ├─ gui_pyqt5/
 └─ docs/
Mỗi thành viên phát triển trên branch riêng (data, mapreduce, db, gui-crud, gui-chart).
Merge vào main sau khi review.
b. Dòng công việc chuẩn
Thu thập dữ liệu
A crawl & làm sạch → push clean_data.csv.
Máy chính lấy về: hdfs dfs -put clean_data.csv /input/project/
Viết & chạy MapReduce
B push code → Máy chính compile & run:
4.  hadoop jar job.jar input output
Kết quả → Database
C push script MySQL/HBase + Sqoop 
GUI
D & E phát triển PyQt5 trên máy mình, test với local DB.
Khi hoàn thiện:
Đổi cấu hình kết nối:
o   host="IP_MAY_CHINH"
o   port=3306  # MySQL
o   phoenix_url="http://IP_MAY_CHINH:8765/"
Push code → Máy chính pull và chạy GUI thực tế.

-- 3.Máy chính (Master Node)
Cài: Hadoop/HDFS, HBase + Phoenix, MySQL, Python + PyQt5.
Mở port:
MySQL: 3306
Phoenix Query Server: 8765
Demo/bảo vệ: chạy GUI trực tiếp trên máy này (hoặc cho các bạn kết nối LAN bằng IP).
 

