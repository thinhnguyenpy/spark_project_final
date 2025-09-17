Tất nhiên, đây là phiên bản mô tả dự án ngắn gọn và súc tích hơn.

Dự án: Phân Tích Hành Vi Khách Hàng IPTV/VOD bằng Apache Spark
Tổng quan
    Dự án xây dựng quy trình ETL và phân tích hành vi người dùng từ dữ liệu log dịch vụ giải trí. Sử dụng Apache Spark (PySpark) để xử lý, biến đổi dữ liệu lớn và lưu trữ        kết quả vào Data Lake/Data Warehouse để trực quan hóa bằng Power BI.

    Luồng xử lý
    Dữ liệu thô → PySpark (Làm sạch) → Data Lake (Parquet) → PySpark (Biến đổi & Tổng hợp) → Data Warehouse (Oracle) → Power BI (Báo cáo).

    Công nghệ chính
    Xử lý dữ liệu: Apache Spark (PySpark)

    Lưu trữ: Data Lake (Parquet), Data Warehouse (Oracle)

    Trực quan hóa: Power BI

    Tự động hóa: SQL Jobs

Cấu trúc thư mục
  pyspark_code/: Mã nguồn PySpark cho ETL.

  clean_data.py: Làm sạch dữ liệu thô, lưu vào Data Lake.

  analysis...py: Biến đổi dữ liệu từ Data Lake và nạp vào Data Warehouse.

  sql_code/: Scripts SQL để tạo bảng và job tự động trong Oracle.

  powerBI_to_visualize/: File báo cáo Power BI.

Tài nguyên
  DataSampleTest, data_lake: https://drive.google.com/drive/folders/1dPABeJtjlicOtklK0TEWGwZ3Fn3-RO-l?usp=drive_link
