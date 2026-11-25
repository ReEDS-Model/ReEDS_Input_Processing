#!/bin/bash
# Opens the ATB inputs dashboard in your browser. Double-click this file (macOS).
# Uses python3 from your PATH; set PY to use another Python that has streamlit.
PY="${PY:-python3}"
cd "$(dirname "$0")/.." || exit 1
if ! "$PY" -c "import streamlit, plotly, ruamel.yaml" 2>/dev/null; then
    echo "$PY is missing streamlit, plotly, or ruamel.yaml."
    echo "Install them with: $PY -m pip install streamlit plotly ruamel.yaml"
    read -r -p "Press Enter to close."
    exit 1
fi
"$PY" -m streamlit run dashboard/app.py --theme.base light
