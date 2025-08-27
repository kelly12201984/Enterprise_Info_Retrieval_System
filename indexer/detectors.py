# CUSTOM_DETECTORS.py (or merge into your existing config-based detectors)
CUSTOM_DETECTORS = {
    "compress":  {"ext_any": {".cw7", ".xml"}, "name_tokens_any": {"compress", "codeware"}},
    "ametank":   {"ext_any": {".mdl", ".xmt_txt"}, "name_tokens_any": {"ametank", "ame"}},
    "cad":       {"ext_any": {".dwg", ".dxf"}},
    "pdf":       {"ext_any": {".pdf"}},

    # New tags
    "excel":     {"ext_any": {".xlsx", ".xlsm", ".xls", ".csv"}},
    "word":      {"ext_any": {".docx", ".doc"}},
    "powerpoint":{"ext_any": {".pptx", ".ppt"}},
    "archive":   {"ext_any": {".zip", ".7z", ".rar"}},
}
