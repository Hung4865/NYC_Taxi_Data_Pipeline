import nbformat as nbf
import os

nb_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\src\machine_learning\customer_segmentation.ipynb"
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbf.read(f, as_version=4)

# Markdown for Elbow Method
elbow_md = """\
### 4.5. Chọn số cụm tối ưu (Elbow Method)
Làm sao để biết chia thành 4 cụm là tốt nhất? Chúng ta sẽ chạy thử K-Means với k từ 2 đến 7 và vẽ biểu đồ Phương sai nội cụm (WSSSE) hoặc Silhouette Score để tìm điểm "Khuỷu tay" (Elbow).
"""

elbow_code = """\
import matplotlib.pyplot as plt

silhouette_scores = []
k_values = range(2, 8)

print("Đang chạy tính toán Elbow Method (Có thể mất 1-2 phút)...")
for k_val in k_values:
    kmeans_temp = KMeans(featuresCol="features", predictionCol="cluster", k=k_val, seed=1)
    model_temp = kmeans_temp.fit(scaled_data)
    predictions_temp = model_temp.transform(scaled_data)
    
    evaluator_temp = ClusteringEvaluator(predictionCol="cluster", featuresCol="features")
    score = evaluator_temp.evaluate(predictions_temp)
    silhouette_scores.append(score)
    print(f"k={k_val} | Silhouette Score: {score:.4f}")

plt.figure(figsize=(8, 5))
plt.plot(k_values, silhouette_scores, marker='o', linestyle='--', color='b')
plt.title("Elbow Method (Silhouette Score) for Optimal k")
plt.xlabel("Number of Clusters (k)")
plt.ylabel("Silhouette Score")
plt.xticks(k_values)
plt.grid(True)
plt.show()

print("-> Nhận xét: Chúng ta sẽ chọn điểm uốn (Elbow) hoặc nơi có điểm Silhouette cao nhất làm số cụm chính thức!")
"""

# Markdown for final charts
charts_md = """\
### 7. Trực quan hóa kết quả Phân cụm (Visualizations)
Vẽ biểu đồ phân tán (Scatter Plot) và biểu đồ đếm (Bar Chart) để nhìn rõ bức tranh kinh doanh.
"""

charts_code = """\
import seaborn as sns

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
sns.countplot(data=preds_pdf, x="cluster", palette="viridis", ax=ax[1])
ax[1].set_title("Number of Trips per Cluster")
ax[1].set_xlabel("Cluster ID")
ax[1].set_ylabel("Count of Trips")

plt.tight_layout()
plt.show()
"""

# Find index to insert elbow (before "5. Đào tạo mô hình K-Means")
elbow_insert_idx = -1
for i, cell in enumerate(nb.cells):
    if "Đào tạo mô hình K-Means" in cell.source:
        elbow_insert_idx = i
        break

if elbow_insert_idx != -1:
    nb.cells.insert(elbow_insert_idx, nbf.v4.new_markdown_cell(elbow_md))
    nb.cells.insert(elbow_insert_idx + 1, nbf.v4.new_code_cell(elbow_code))

# Insert charts at the very end
nb.cells.append(nbf.v4.new_markdown_cell(charts_md))
nb.cells.append(nbf.v4.new_code_cell(charts_code))

with open(nb_path, 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
print("Updated notebook with Elbow Method and Charts!")
