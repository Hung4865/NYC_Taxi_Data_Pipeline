import json
path = "src/machine_learning/customer_segmentation.ipynb"
with open(path, "r", encoding="utf-8") as f:
    nb = json.load(f)

old_path = r"D:\Apps\Intellj\files\IntelliJ IDEA 2025.3.1\jbr"
new_path = r"d:\School\spark\NYC_Taxi_Data_Pipeline-main\jdk\jdk-17.0.10+7"

for i, cell in enumerate(nb["cells"]):
    if cell["cell_type"] == "code" and "JAVA_HOME" in "".join(cell["source"]):
        new_source = []
        for line in cell["source"]:
            new_source.append(line.replace(old_path, new_path))
        nb["cells"][i]["source"] = new_source
        break

with open(path, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)
print("Updated JAVA_HOME successfully!")
