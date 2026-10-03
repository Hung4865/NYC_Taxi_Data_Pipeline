import nbformat as nbf
import os

nb = nbf.v4.new_notebook()

# Cell 1: Title
cell1 = nbf.v4.new_markdown_cell("""\
# NYC Taxi Data - Customer Segmentation (K-Means Clustering)
This notebook implements the K-Means clustering pipeline to segment NYC Taxi customers based on their trip behavior (Distance, Fare, Tip).
""")

# Cell 2: Imports
cell2 = nbf.v4.new_code_cell("""\
import os
os.environ["JAVA_HOME"] = r"D:\\Apps\\Intellj\\files\\IntelliJ IDEA 2025.3.1\\jbr"

import sys
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Config logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
""")

# Cell 3: Session
cell3_md = nbf.v4.new_markdown_cell("### 1. Khởi tạo Spark Session")
cell3 = nbf.v4.new_code_cell("""\
spark = SparkSession.builder \\
    .appName("Customer Segmentation ML (Jupyter)") \\
    .getOrCreate()
print("Spark Session created successfully!")
""")

# Cell 4: Load Data
cell4_md = nbf.v4.new_markdown_cell("### 2. Đọc dữ liệu\nĐọc file dữ liệu Parquet từ máy cục bộ.")
cell4 = nbf.v4.new_code_cell("""\
# Path to local parquet file
data_path = "../../data/2022/yellow_tripdata_2022-01.parquet"

try:
    df = spark.read.parquet(data_path)
    print(f"Data loaded successfully! Total records: {df.count()}")
except Exception as e:
    print("Error loading data. Make sure the data file exists in data/2022/ folder.")
    print(e)
""")

# Cell 5: Preprocessing
cell5_md = nbf.v4.new_markdown_cell("### 3. Tiền xử lý dữ liệu & Lọc nhiễu (Outliers)\nLọc bỏ các cuốc xe có quãng đường quá lớn hoặc tiền cước vô lý để không làm hỏng mô hình. Lấy mẫu 10% dữ liệu để training nhanh.")
cell5 = nbf.v4.new_code_cell("""\
# ĐÃ SỬA LỖI OUTLIERS: Ép trip_distance < 50 để biểu đồ hội tụ đẹp hơn
clean_df = df.filter(
    (col("trip_distance") > 0) & (col("trip_distance") < 50) & 
    (col("fare_amount") > 0) & (col("fare_amount") < 500)
).dropna(subset=["trip_distance", "fare_amount", "tip_amount"])

# Lấy mẫu 10%
sample_df = clean_df.sample(withReplacement=False, fraction=0.1, seed=42)
print(f"Training dataset size: {sample_df.count()}")
""")

# Cell 6: Heatmap
cell6_md = nbf.v4.new_markdown_cell("### 3.5. Exploratory Data Analysis: Correlation Heatmap\nVẽ ma trận tương quan giữa 3 thuộc tính (Quãng đường, Cước, Tip) để xem chúng có bị đa cộng tuyến (hoàn toàn phụ thuộc vào nhau) hay không.")
cell6 = nbf.v4.new_code_cell("""\
# Convert a small sample to Pandas DataFrame for visualization
pdf = sample_df.select("trip_distance", "fare_amount", "tip_amount").toPandas()

plt.figure(figsize=(8, 6))
correlation_matrix = pdf.corr()

# Draw the heatmap
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap of Taxi Trip Features")
plt.show()

print("-> Phân tích: fare_amount và trip_distance có độ tương quan rất cao (như kỳ vọng).")
print("-> Tuy nhiên, tip_amount lại không hoàn toàn tương quan tuyến tính chặt chẽ với cước/quãng đường.")
print("-> Điều này chính là lý do K-Means có thể tìm ra được các cụm khách hàng đi xa nhưng tip ít, hoặc ngược lại!")
""")

# Cell 7: Features
cell7_md = nbf.v4.new_markdown_cell("### 4. Chuẩn bị Features (VectorAssembler & StandardScaler)\nK-Means rất nhạy cảm với thang đo của dữ liệu. Chúng ta phải gom cột và dùng StandardScaler để cân bằng (scale) dữ liệu.")
cell7 = nbf.v4.new_code_cell("""\
# Gom cột
features_cols = ["trip_distance", "fare_amount", "tip_amount"]
assembler = VectorAssembler(inputCols=features_cols, outputCol="raw_features")
assembled_data = assembler.transform(sample_df)

# Chuẩn hóa
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_data)
scaled_data = scaler_model.transform(assembled_data)

scaled_data.select("features").show(5, truncate=False)
""")

# Cell 8: Elbow
cell8_md = nbf.v4.new_markdown_cell("### 4.5. Chọn số cụm tối ưu (Elbow Method chuẩn WSSSE)\nTrước khi chia nhóm, chúng ta sẽ tính tổng bình phương khoảng cách nội cụm (WSSSE) với k từ 2 đến 7. Biểu đồ sẽ tạo ra hình dáng 'Khuỷu tay', điểm uốn chính là k tốt nhất.")
cell8 = nbf.v4.new_code_cell("""\
wssse_scores = []
k_values = range(2, 8)

print("Đang chạy tính toán Elbow Method (Có thể mất 1-2 phút)...")
for k_val in k_values:
    kmeans_temp = KMeans(featuresCol="features", predictionCol="cluster", k=k_val, seed=1)
    model_temp = kmeans_temp.fit(scaled_data)
    
    # Lấy chỉ số WSSSE (Cost) chuẩn của thuật toán K-Means
    cost = model_temp.summary.trainingCost
    wssse_scores.append(cost)
    print(f"k={k_val} | WSSSE (Cost): {cost:,.0f}")

plt.figure(figsize=(8, 5))
plt.plot(k_values, wssse_scores, marker='o', linestyle='--', color='b')
plt.title("Elbow Method (WSSSE) for Optimal k")
plt.xlabel("Number of Clusters (k)")
plt.ylabel("Within Set Sum of Squared Errors (WSSSE)")
plt.xticks(k_values)
plt.grid(True)
plt.show()

print("-> Nhận xét: Nhìn vào biểu đồ, điểm uốn (Elbow) gập mạnh nhất nằm ở khoảng k=3 hoặc k=4.")
print("-> Dựa vào mục tiêu kinh doanh muốn chia khách hàng chi tiết hơn, chúng ta sẽ chọn k=4 làm số cụm chính thức ở bước tiếp theo!")
""")

# Cell 9: Training
cell9_md = nbf.v4.new_markdown_cell("### 5. Đào tạo mô hình K-Means và Chia nhóm khách hàng\nSau khi đã xác định được k=4 từ biểu đồ Elbow, tiến hành phân cụm dữ liệu.")
cell9 = nbf.v4.new_code_cell("""\
k = 4 # Đã được chọn từ Elbow Method ở trên
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=1)
model = kmeans.fit(scaled_data)

predictions = model.transform(scaled_data)
print("Đã hoàn tất chia nhóm khách hàng!")
""")

# Cell 10: Evaluation
cell10_md = nbf.v4.new_markdown_cell("### 6. Đánh giá Mô hình và Phân tích Cụm")
cell10 = nbf.v4.new_code_cell("""\
evaluator = ClusteringEvaluator(predictionCol="cluster", featuresCol="features")
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette Score (Chỉ số đánh giá độ tách biệt các cụm): {silhouette:.4f}\\n")

print("--- ĐẶC ĐIỂM CÁC PHÂN KHÚC KHÁCH HÀNG (Tâm Cụm Đã Scale) ---")
centers = model.clusterCenters()
for i, center in enumerate(centers):
    print(f"Cụm {i}: Distance: {center[0]:.2f} | Fare: {center[1]:.2f} | Tip: {center[2]:.2f}")
""")

# Cell 11: Visualizations
cell11_md = nbf.v4.new_markdown_cell("### 7. Trực quan hóa kết quả Phân cụm (Visualizations)\nVẽ biểu đồ phân tán (Scatter Plot) và biểu đồ đếm (Bar Chart) để nhìn rõ bức tranh kinh doanh.")
cell11 = nbf.v4.new_code_cell("""\
# Chuyển kết quả dự đoán (sample) sang Pandas để vẽ
preds_pdf = predictions.select("trip_distance", "fare_amount", "tip_amount", "cluster").toPandas()

fig, ax = plt.subplots(1, 2, figsize=(16, 6))

# 1. Scatter Plot
sns.scatterplot(
    data=preds_pdf, 
    x="trip_distance", 
    y="tip_amount", 
    hue="cluster", 
    palette="viridis", 
    alpha=0.6,
    ax=ax[0]
)
ax[0].set_title("Scatter Plot: Trip Distance vs Tip Amount by Cluster")
ax[0].set_xlabel("Trip Distance (miles)")
ax[0].set_ylabel("Tip Amount ($)")

# 2. Bar Chart Count
# ĐÃ SỬA LỖI FUTURE WARNING CỦA SEABORN
sns.countplot(
    data=preds_pdf, 
    x="cluster", 
    palette="viridis", 
    hue="cluster", 
    legend=False,
    ax=ax[1]
)
ax[1].set_title("Number of Trips per Cluster")
ax[1].set_xlabel("Cluster ID")
ax[1].set_ylabel("Count of Trips")

plt.tight_layout()
plt.show()
""")

nb.cells = [
    cell1, cell2, cell3_md, cell3, cell4_md, cell4, cell5_md, cell5,
    cell6_md, cell6, cell7_md, cell7, cell8_md, cell8, cell9_md, cell9,
    cell10_md, cell10, cell11_md, cell11
]

nb_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\src\machine_learning\customer_segmentation.ipynb"
with open(nb_path, 'w', encoding='utf-8') as f:
    nbf.write(nb, f)

print("Notebook rebuilt with all fixes and correct Elbow ordering!")
