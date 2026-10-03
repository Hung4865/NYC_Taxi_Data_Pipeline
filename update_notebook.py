import nbformat as nbf
import os

nb_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\src\machine_learning\customer_segmentation.ipynb"
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbf.read(f, as_version=4)

# Find the first code cell and prepend the JAVA_HOME setup
for cell in nb.cells:
    if cell.cell_type == 'code':
        java_setup = 'import os\nos.environ["JAVA_HOME"] = r"D:\\Apps\\Intellj\\files\\IntelliJ IDEA 2025.3.1\\jbr"\n\n'
        if "JAVA_HOME" not in cell.source:
            cell.source = java_setup + cell.source
        break

with open(nb_path, 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
print("Notebook updated with JAVA_HOME")
