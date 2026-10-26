#!/usr/bin/env python3
"""Static package verification; does not validate live market integrations."""
from pathlib import Path
import ast, hashlib, zipfile, sys
root=Path(__file__).resolve().parent
app=root/'app.py'
source=app.read_text(encoding='utf-8')
ast.parse(source, filename=str(app))
compile(source, str(app), 'exec')
required=['_render_time_machine_risk_lab','_render_bt_lab','smart_flow_signal','telegram_send','get_backtest_5m','turtle_position_size','live_dashboard']
missing=[name for name in required if f'def {name}(' not in source]
if missing:
    raise SystemExit('FAIL missing expected functions: '+', '.join(missing))
if 'max_value=5.0' not in source or 'configurable staging ceiling: 5%' not in source:
    raise SystemExit('FAIL expected staging risk range/label not found')
if "upstream skill" not in (root/"README.md").read_text(encoding="utf-8"):
    raise SystemExit('FAIL Time Machine limitation not documented')
print('PASS: AST parse and bytecode compile')
print('PASS: expected core function definitions present')
print('PASS: staging risk configuration and Time Machine limitation documented')
print('SHA-256 app.py:',hashlib.sha256(app.read_bytes()).hexdigest())
print('NOTE: Streamlit UI, SiftingIO, Telegram and trading performance are NOT tested by this script.')
