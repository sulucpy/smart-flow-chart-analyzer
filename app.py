import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

st.set_page_config(page_title="Smart Flow Analyzer", layout="wide")
st.title("📊 Smart Flow Chart Analyzer")
st.caption("Python • Gold / NIFTY / Oil • BUY / SELL / WAIT")

def prep(df):
    m={}
    for c in df.columns:
        k=str(c).lower().replace(" ","").replace("_","")
        if k in ("date","datetime","timestamp","time"): m[c]="datetime"
        elif k=="open": m[c]="open"
        elif k=="high": m[c]="high"
        elif k=="low": m[c]="low"
        elif k=="close": m[c]="close"
        elif k in ("volume","vol"): m[c]="volume"
    df=df.rename(columns=m).copy()
    need=["open","high","low","close"]
    if not all(c in df for c in need):
        raise ValueError("CSV needs Open, High, Low and Close columns.")
    if "datetime" in df:
        df["datetime"]=pd.to_datetime(df["datetime"],errors="coerce")
    else:
        df["datetime"]=range(len(df))
    for c in need: df[c]=pd.to_numeric(df[c],errors="coerce")
    return df.dropna(subset=need).sort_values("datetime").reset_index(drop=True)

def calc(df):
    x=df.copy()
    x["ema20"]=x.close.ewm(span=20,adjust=False).mean()
    x["ema50"]=x.close.ewm(span=50,adjust=False).mean()
    x["ema200"]=x.close.ewm(span=200,adjust=False).mean()
    d=x.close.diff(); gain=d.clip(lower=0); loss=-d.clip(upper=0)
    ag=gain.ewm(alpha=1/14,adjust=False).mean()
    al=loss.ewm(alpha=1/14,adjust=False).mean()
    rs=ag/al.replace(0,np.nan)
    x["rsi"]=100-100/(1+rs)
    pc=x.close.shift()
    tr=pd.concat([(x.high-x.low),(x.high-pc).abs(),(x.low-pc).abs()],axis=1).max(axis=1)
    x["atr"]=tr.ewm(alpha=1/14,adjust=False).mean()
    up=x.high.diff(); dn=-x.low.diff()
    plus=np.where((up>dn)&(up>0),up,0)
    minus=np.where((dn>up)&(dn>0),dn,0)
    pdi=100*pd.Series(plus,index=x.index).ewm(alpha=1/14,adjust=False).mean()/x.atr
    mdi=100*pd.Series(minus,index=x.index).ewm(alpha=1/14,adjust=False).mean()/x.atr
    dx=100*(pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan)
    x["di_plus"]=pdi; x["di_minus"]=mdi
    x["adx"]=dx.ewm(alpha=1/14,adjust=False).mean()
    x["ph"]=x.high.shift().rolling(10).max()
    x["pl"]=x.low.shift().rolling(10).min()
    rng=x.high-x.low
    x["body"]=(x.close-x.open).abs()/rng.replace(0,np.nan)
    bullbody=(x.close>x.open)&(x.body>=.50)&(x.close>=x.high-rng*.25)
    bearbody=(x.close<x.open)&(x.body>=.50)&(x.close<=x.low+rng*.25)
    x["buy"]=(x.close>x.ph)&(x.ema20>x.ema50)&(x.close>x.ema20)&(x.adx>18)&(x.di_plus>x.di_minus)&(x.rsi>50)&bullbody
    x["sell"]=(x.close<x.pl)&(x.ema20<x.ema50)&(x.close<x.ema20)&(x.adx>18)&(x.di_minus>x.di_plus)&(x.rsi<50)&bearbody
    return x

def bt(x,slmult=.6,target=1.5):
    out=[]; i=0
    while i<len(x)-1:
        r=x.iloc[i]
        if not(r.buy or r.sell): i+=1; continue
        buy=bool(r.buy); entry=float(r.close); atr=float(r.atr)
        if not np.isfinite(atr) or atr<=0: i+=1; continue
        sl=min(r.low-atr*slmult,entry-atr*slmult) if buy else max(r.high+atr*slmult,entry+atr*slmult)
        risk=entry-sl if buy else sl-entry
        tp=entry+risk*target if buy else entry-risk*target
        res=None; j=i+1
        for j in range(i+1,len(x)):
            h,l=float(x.iloc[j].high),float(x.iloc[j].low)
            if buy:
                if l<=sl: res=-1.; break
                if h>=tp: res=target; break
            else:
                if h>=sl: res=-1.; break
                if l<=tp: res=target; break
        if res is None: res=0.
        out.append({"Direction":"BUY" if buy else "SELL","Entry":entry,"SL":sl,"TP":tp,"R":res,"Time":r.datetime})
        i=max(j+1,i+1)
    return pd.DataFrame(out)

with st.sidebar:
    st.header("Settings")
    slmult=st.number_input("SL ATR",.2,2.,.6,.05)
    target=st.number_input("Backtest Target (R)",.5,4.,1.5,.25)
    showema=st.checkbox("Show EMA",True)
    st.info("Use a clean OHLC CSV for reliable chronological backtesting.")

f=st.file_uploader("Upload OHLC CSV",type="csv")
if not f:
    st.warning("Upload your TradingView CSV to start.")
    st.stop()

try: df=prep(pd.read_csv(f)); x=calc(df)
except Exception as e:
    st.error(str(e)); st.stop()

r=x.iloc[-1]
action="🟢 BUY NOW" if r.buy else "🔴 SELL NOW" if r.sell else "⚪ WAIT"
quality="A+" if r.buy or r.sell else "WAIT"

a,b,c,d=st.columns(4)
a.metric("ACTION",action); b.metric("QUALITY",quality)
c.metric("RSI",f"{r.rsi:.1f}"); d.metric("ADX",f"{r.adx:.1f}")

fig=go.Figure(go.Candlestick(x=x.datetime,open=x.open,high=x.high,low=x.low,close=x.close,name="Price"))
if showema:
    for c in ["ema20","ema50","ema200"]: fig.add_trace(go.Scatter(x=x.datetime,y=x[c],name=c.upper(),mode="lines"))
fig.update_layout(template="plotly_dark",height=600,xaxis_rangeslider_visible=False)
st.plotly_chart(fig,use_container_width=True)

st.subheader("Current Analysis")
st.write(f"**Price:** {r.close:.2f}  |  **Trend:** {'BULLISH' if r.ema20>r.ema50 else 'BEARISH'}  |  **Momentum:** {'BUY' if r.di_plus>r.di_minus else 'SELL'}")

if r.buy or r.sell:
    entry=float(r.close); atr=float(r.atr)
    sl=min(r.low-atr*slmult,entry-atr*slmult) if r.buy else max(r.high+atr*slmult,entry+atr*slmult)
    risk=entry-sl if r.buy else sl-entry
    tps=[entry+risk*k if r.buy else entry-risk*k for k in [.5,1,1.5,2]]
    st.success(f"{action} | Entry {entry:.2f} | SL {sl:.2f} | TP1 {tps[0]:.2f} | TP2 {tps[1]:.2f} | TP3 {tps[2]:.2f} | TP4 {tps[3]:.2f}")
else:
    st.info("No complete setup on the latest candle. WAIT.")

st.subheader("Backtest")
tr=bt(x,slmult,target)
if len(tr)==0:
    st.warning("No completed trades with these rules.")
else:
    wins=int((tr.R>0).sum()); losses=int((tr.R<0).sum()); net=float(tr.R.sum())
    gp=float(tr.loc[tr.R>0,"R"].sum()); gl=abs(float(tr.loc[tr.R<0,"R"].sum()))
    pf=gp/gl if gl else np.inf
    q1,q2,q3,q4,q5=st.columns(5)
    q1.metric("Trades",len(tr)); q2.metric("Win Rate",f"{wins/len(tr)*100:.1f}%")
    q3.metric("Net R",f"{net:.2f}R"); q4.metric("Profit Factor",f"{pf:.2f}" if np.isfinite(pf) else "∞")
    q5.metric("Wins/Losses",f"{wins}/{losses}")
    tr["Equity R"]=tr.R.cumsum()
    ef=go.Figure(go.Scatter(x=range(len(tr)),y=tr["Equity R"],mode="lines+markers",name="Equity R"))
    ef.update_layout(template="plotly_dark",height=350,xaxis_title="Trade",yaxis_title="Cumulative R")
    st.plotly_chart(ef,use_container_width=True)
    st.dataframe(tr,use_container_width=True)

st.caption("Research/education tool. Backtest results depend on the uploaded data and execution assumptions; they are not a guarantee of future performance.")
