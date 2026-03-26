# AGOL Web Map Extractor

## Overview
AGOL Web Map Extractor is a Python tool that exports extractable feature layers from an ArcGIS Online or ArcGIS Enterprise web map into a local File Geodatabase.

It is designed for GIS analysts and administrators who need a faster, more repeatable way to back up or reuse web map data.

Enter Portal URL: https://www.arcgis.com
Enter Web Map ID: xxxxxxxx
Output Folder: C:\GIS\Exports

---

## Why this project matters
Manual export of web map layers can be slow, repetitive, and inconsistent, especially when a map contains many operational layers. This tool helps standardize that process and provides a clear audit trail of what succeeded and what failed.

---

## Key features
- Interactive mode for everyday users
- Command-line mode for repeatable workflows
- Recursive discovery of operational layers, including group layers
- ArcPy-first export path with ArcGIS API for Python fallback
- Log, CSV, and TXT summaries for traceability
- Continues on failure unless you choose to stop on first error

---

## Repository structure
```bash
agol-webmap-extractor/
├── README.md
├── requirements.txt
├── .gitignore
├── LICENSE
├── src/
│   └── agol_webmap_extractor.py
└── examples/
    └── agol_webmap_to_fgdb_jupyter_selectable.ipynb
```

---

## Recommended environment
Best run from a cloned **ArcGIS Pro Python** environment so you have:
- `arcpy`
- `arcgis` (ArcGIS API for Python)
- `pandas`

Example setup from **ArcGIS Pro Python Command Prompt**:

```bash
conda create --clone arcgispro-py3 --name wm_export_env
conda activate wm_export_env
conda install pandas
conda install -c esri arcgis
```

---

## Install dependencies
```bash
pip install -r requirements.txt
```

---

## How to run
### Interactive mode
```bash
python src/agol_webmap_extractor.py
```

The script will prompt for:
- Portal URL
- Web map item ID
- Output folder
- Output FGDB name
- Sign-in requirement
- Username and password if needed
- Debug logging choice
- Stop-on-error choice

### Command-line mode
```bash
python src/agol_webmap_extractor.py \
  --portal https://www.arcgis.com \
  --webmap-id YOUR_WEBMAP_ITEM_ID \
  --username YOUR_USERNAME \
  --output-folder C:\temp\webmap_export \
  --gdb-name my_webmap_dump.gdb \
  --debug
```

If you omit `--password`, the script prompts you securely.

---

## Outputs
The script creates:
- `webmap_export.log`
- `webmap_export_summary.csv`
- `webmap_export_summary.txt`
- A local `.gdb` containing exported layers that succeeded

---

## Limitations
- Best for `FeatureServer` layers and some queryable `MapServer` sublayers
- Not intended for basemaps, vector tiles, imagery, WMS, scene layers, or other non-feature operational layers
- Fallback SEDF export does not preserve everything you would get from a full geodatabase-native workflow, such as attachments or some advanced geodatabase behavior
- Some services cannot be exported if export or query access is restricted by the owner or administrator

---

## Included notebook
A notebook-friendly selectable export workflow is included in `examples/` for users who prefer working in ArcGIS Pro notebooks.

---

## Author
**Muyiwa Adeniyi**  
GIS Specialist | Spatial Analytics | GIS Automation
