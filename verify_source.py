import ast, pathlib, sys
p = pathlib.Path(__file__).with_name('app.py')
s = p.read_text(encoding='utf-8')
ast.parse(s)
compile(s, str(p), 'exec')
for name in ['london_session_snapshot','london_killzone_backtest','render_london_killzone_live','render_london_killzone_backtest_ui','live_dashboard','_render_bt_lab']:
    if name not in s:
        raise SystemExit(f'Missing required source marker: {name}')
print(f'PASS: {p.name}, {len(s.splitlines())} lines, syntax/compile and required markers OK')
