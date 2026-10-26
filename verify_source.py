import ast, pathlib
p=pathlib.Path(__file__).with_name('app.py')
s=p.read_text(encoding='utf-8')
ast.parse(s); compile(s,str(p),'exec')
for marker in ['london_session_snapshot','london_killzone_backtest','render_london_killzone_live','_render_london_session_clock','_render_london_candlestick_map','Smart View — all checks in one place','conditional watch plan']:
    assert marker in s, f'Missing {marker}'
print('PASS: syntax/compile and Smart View + London module markers')
