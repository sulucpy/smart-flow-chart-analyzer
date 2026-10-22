"""Local static verification for Smart Flow Gold V3 PRO candidate.
This checks syntax and selected source markers only; it does not run Streamlit or live services.
"""
import ast
import hashlib
from pathlib import Path

source = Path(__file__).with_name("app.py")
text = source.read_text(encoding="utf-8")
ast.parse(text, filename=str(source))
compile(text, str(source), "exec")
tree = ast.parse(text)
top_defs = [n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
markers = {
    "Quick Trade": "Quick Trade",
    "VWAP": "VWAP",
    "Liquidity Sweep": "liquidity_enhanced",
    "Telegram alert": "maybe_alert",
    "Paper Trading": "maybe_log_paper_signal",
    "Paper trade updates": "update_paper_trades",
    "Next Trigger map": "next_trigger_map",
}
missing = [label for label, marker in markers.items() if marker not in text]
print(f"Syntax/compile: PASS")
print(f"Source lines: {len(text.splitlines())}")
print(f"Top-level functions/classes: {len(top_defs)}")
print(f"SHA-256: {hashlib.sha256(text.encode()).hexdigest()}")
if missing:
    print("Missing source markers:", ", ".join(missing))
    raise SystemExit(1)
print(f"Selected source markers: PASS ({len(markers)})")
print("LIMIT: this is not a functional, Streamlit-launch, WebSocket, Telegram, or trading validation.")
