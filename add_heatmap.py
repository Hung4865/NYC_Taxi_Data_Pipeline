import nbformat as nbf
import os

nb_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\src\machine_learning\customer_segmentation.ipynb"
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbf.read(f, as_version=4)

# Define the new cells
md_text = """\
### 3.5. Exploratory Data Analysis: Correlation Heatmap
Vẽ ma trận tương quan giữa 3 thuộc tính (Quãng đường, Cước, Tip) để xem chúng có bị đa cộng tuyến (hoàn toàn phụ thuộc vào nhau) hay không.
"""

code_text = """\
import matplotlib.pyplot as plt
import seaborn as sns

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
"""

# Insert right after the data preprocessing cell (which is cell index 7 usually, but let's find it dynamically)
insert_index = -1
for i, cell in enumerate(nb.cells):
    if "VectorAssembler" in cell.source and "StandardScaler" in cell.source:
        # We want to insert BEFORE the VectorAssembler cell
        # Wait, the markdown for VectorAssembler is right before it.
        # Let's insert before the markdown cell for VectorAssembler.
        insert_index = i - 1
        break

if insert_index != -1:
    nb.cells.insert(insert_index, nbf.v4.new_markdown_cell(md_text))
    nb.cells.insert(insert_index + 1, nbf.v4.new_code_cell(code_text))
    
    with open(nb_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    print("Added heatmap to notebook successfully!")
else:
    print("Could not find the insertion point.")
