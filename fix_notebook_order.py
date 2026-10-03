import nbformat as nbf

nb_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\src\machine_learning\customer_segmentation.ipynb"
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbf.read(f, as_version=4)

# Create a dictionary to hold the cells based on their content markers
cell_dict = {}

for cell in nb.cells:
    src = cell.source
    if "NYC Taxi Data - Customer Segmentation" in src:
        cell_dict['title'] = cell
    elif "import sys" in src and "JAVA_HOME" in src:
        cell_dict['imports'] = cell
    elif "1. Khởi tạo Spark Session" in src:
        cell_dict['step1_md'] = cell
    elif "SparkSession.builder" in src and "import sys" not in src:
        cell_dict['step1_code'] = cell
    elif "2. Đọc dữ liệu" in src:
        cell_dict['step2_md'] = cell
    elif "data_path =" in src:
        cell_dict['step2_code'] = cell
    elif "3. Tiền xử lý dữ liệu" in src:
        cell_dict['step3_md'] = cell
    elif "clean_df =" in src:
        cell_dict['step3_code'] = cell
    elif "3.5. Exploratory Data Analysis" in src:
        cell_dict['step3.5_md'] = cell
    elif "sns.heatmap" in src:
        cell_dict['step3.5_code'] = cell
    elif "4. Chuẩn bị Features" in src:
        cell_dict['step4_md'] = cell
    elif "assembler = VectorAssembler" in src and "import" not in src:
        cell_dict['step4_code'] = cell
    elif "4.5. Chọn số cụm tối ưu" in src:
        cell_dict['step4.5_md'] = cell
    elif "k_values = range" in src:
        cell_dict['step4.5_code'] = cell
    elif "5. Đào tạo mô hình K-Means" in src:
        cell_dict['step5_md'] = cell
    elif "k = 4" in src and "KMeans(" in src:
        cell_dict['step5_code'] = cell
    elif "6. Đánh giá Mô hình" in src:
        cell_dict['step6_md'] = cell
    elif "ClusteringEvaluator(" in src and "import" not in src and "evaluator_temp" not in src:
        cell_dict['step6_code'] = cell
    elif "7. Trực quan hóa" in src:
        cell_dict['step7_md'] = cell
    elif "sns.scatterplot" in src:
        cell_dict['step7_code'] = cell

# Reassemble in the correct order
ordered_keys = [
    'title', 'imports',
    'step1_md', 'step1_code',
    'step2_md', 'step2_code',
    'step3_md', 'step3_code',
    'step3.5_md', 'step3.5_code',
    'step4_md', 'step4_code',
    'step4.5_md', 'step4.5_code',
    'step5_md', 'step5_code',
    'step6_md', 'step6_code',
    'step7_md', 'step7_code'
]

new_cells = []
for k in ordered_keys:
    if k in cell_dict:
        new_cells.append(cell_dict[k])
    else:
        print(f"Warning: Missing {k}")

if len(new_cells) == len(ordered_keys):
    nb.cells = new_cells
    with open(nb_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    print("Notebook cells reordered successfully!")
else:
    print("Could not reorder all cells, something is missing.")
