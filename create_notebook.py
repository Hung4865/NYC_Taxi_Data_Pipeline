import nbformat as nbf

nb = nbf.v4.new_notebook()

text_1 = """\
# NYC Taxi Data - Customer Segmentation (K-Means Clustering)
This notebook implements the K-Means clustering pipeline to segment NYC Taxi customers based on their trip behavior (Distance, Fare, Tip).
"""

code_1 = """\
import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Config logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
"""

text_2 = """\
### 1. Khởi tạo Spark Session
"""

code_2 = """\
spark = SparkSession.builder \\
    .appName("Customer Segmentation ML (Jupyter)") \\
    .getOrCreate()
print("Spark Session created successfully!")
"""

text_3 = """\
### 2. Đọc dữ liệu
Đọc file dữ liệu Parquet từ máy cục bộ.
"""

code_3 = """\
# Path to local parquet file
data_path = "../../data/2022/yellow_tripdata_2022-01.parquet"

try:
    df = spark.read.parquet(data_path)
    print(f"Data loaded successfully! Total records: {df.count()}")
except Exception as e:
    print("Error loading data. Make sure the data file exists in data/2022/ folder.")
    print(e)
"""

text_4 = """\
### 3. Tiền xử lý dữ liệu & Lọc nhiễu (Outliers)
Lọc bỏ các cuốc xe có quãng đường quá lớn hoặc tiền cước vô lý để không làm hỏng mô hình. Lấy mẫu 10% dữ liệu để training nhanh.
"""

code_4 = """\
clean_df = df.filter(
    (col("trip_distance") > 0) & (col("trip_distance") < 100) & 
    (col("fare_amount") > 0) & (col("fare_amount") < 500)
).dropna(subset=["trip_distance", "fare_amount", "tip_amount"])

# Lấy mẫu 10%
sample_df = clean_df.sample(withReplacement=False, fraction=0.1, seed=42)
print(f"Training dataset size: {sample_df.count()}")
"""

text_5 = """\
### 4. Chuẩn bị Features (VectorAssembler & StandardScaler)
K-Means rất nhạy cảm với thang đo của dữ liệu. Chúng ta phải gom cột và dùng StandardScaler để cân bằng (scale) dữ liệu.
"""

code_5 = """\
# Gom cột
features_cols = ["trip_distance", "fare_amount", "tip_amount"]
assembler = VectorAssembler(inputCols=features_cols, outputCol="raw_features")
assembled_data = assembler.transform(sample_df)

# Chuẩn hóa
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_data)
scaled_data = scaler_model.transform(assembled_data)

scaled_data.select("features").show(5, truncate=False)
"""

text_6 = """\
### 5. Đào tạo mô hình K-Means
"""

code_6 = """\
k = 4 # Chia làm 4 cụm
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=1)
model = kmeans.fit(scaled_data)

predictions = model.transform(scaled_data)
"""

text_7 = """\
### 6. Đánh giá Mô hình và Phân tích Cụm
"""

code_7 = """\
evaluator = ClusteringEvaluator(predictionCol="cluster", featuresCol="features")
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette Score: {silhouette:.4f}")

print("\\n--- ĐẶC ĐIỂM CÁC PHÂN KHÚC KHÁCH HÀNG (Tâm Cụm Đã Scale) ---")
centers = model.clusterCenters()
for i, center in enumerate(centers):
    print(f"Cụm {i}: Distance: {center[0]:.2f} | Fare: {center[1]:.2f} | Tip: {center[2]:.2f}")
"""

nb['cells'] = [
    nbf.v4.new_markdown_cell(text_1),
    nbf.v4.new_code_cell(code_1),
    nbf.v4.new_markdown_cell(text_2),
    nbf.v4.new_code_cell(code_2),
    nbf.v4.new_markdown_cell(text_3),
    nbf.v4.new_code_cell(code_3),
    nbf.v4.new_markdown_cell(text_4),
    nbf.v4.new_code_cell(code_4),
    nbf.v4.new_markdown_cell(text_5),
    nbf.v4.new_code_cell(code_5),
    nbf.v4.new_markdown_cell(text_6),
    nbf.v4.new_code_cell(code_6),
    nbf.v4.new_markdown_cell(text_7),
    nbf.v4.new_code_cell(code_7),
]

with open('src/machine_learning/customer_segmentation.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
print("Notebook created successfully!")
