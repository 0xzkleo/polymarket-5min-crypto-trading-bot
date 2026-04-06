"""
view_paper_trades.py  --  Simulation trade tracker and analytics dashboard

Usage:
    python view_paper_trades.py                   # full report
    python view_paper_trades.py --last 20         # last 20 trades only
    python view_paper_trades.py --summary         # stats only, no trade table
    python view_paper_trades.py --csv             # export to paper_trades.csv
    python view_paper_trades.py --watch           # auto-refresh every 10s
"""

import json, sys, os, time, argparse
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any, Optional

TRADES_FILE = Path(__file__).parent / "paper_trades.json"

def load_trades(path=TRADES_FILE):
    if not Path(path).exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception as e:
        print(f"Error: {e}")
        return []

def compute_stats(trades):
    if not trades:
        return {}
    settled  = [t for t in trades if t.get("outcome") in ("WIN","LOSS")]
    wins     = [t for t in settled if t["outcome"]=="WIN"]
    losses   = [t for t in settled if t["outcome"]=="LOSS"]
    pending  = [t for t in trades  if t.get("outcome")=="PENDING"]
    n = len(trades); ns = len(settled); nw = len(wins); nl = len(losses)
    wr = nw/ns*100 if ns else 0
    pnls = [t.get("pnl_usd",0) for t in trades]
    total_pnl = sum(pnls)
    avg_w  = sum(t.get("pnl_usd",0) for t in wins)/nw     if nw else 0
    avg_l  = sum(t.get("pnl_usd",0) for t in losses)/nl   if nl else 0
    pf     = abs(avg_w*nw / (avg_l*nl)) if nl and avg_l else float("inf")
    # streaks
    best=worst=cur=0; ctype=None
    for t in settled:
        o=t["outcome"]
        if o==ctype: cur+=1
        else: ctype=o; cur=1
        if ctype=="WIN": best=max(best,cur)
        else:            worst=max(worst,cur)
    lstreak=0; ltype=None
    for t in reversed(settled):
        o=t["outcome"]
        if ltype is None: ltype=o
        if o==ltype: lstreak+=1
        else: break
    # drawdown
    eq=pk=dd=0
    for p in pnls:
        eq+=p; pk=max(pk,eq); dd=max(dd,pk-eq)
    # direction
    longs  = [t for t in settled if t.get("direction")=="LONG"]
    shorts = [t for t in settled if t.get("direction")=="SHORT"]
    lwr = sum(1 for t in longs  if t["outcome"]=="WIN")/len(longs)*100  if longs  else 0
    swr = sum(1 for t in shorts if t["outcome"]=="WIN")/len(shorts)*100 if shorts else 0
    # ml
    mle  = [t for t in trades if t.get("ml_edge",0)>0]
    aedge= sum(t["ml_edge"] for t in mle)/len(mle) if mle else 0
    apup = sum(t.get("ml_p_up",0) for t in trades)/n if n else 0
    # vol regime
    rs = defaultdict(lambda:{"total":0,"wins":0})
    for t in settled:
        r=t.get("vol_regime") or "unknown"
        rs[r]["total"]+=1
        if t["outcome"]=="WIN": rs[r]["wins"]+=1
    # session duration
    tss=[]
    for t in trades:
        try: tss.append(datetime.fromisoformat(t["timestamp"]))
        except: pass
    dur=""
    if len(tss)>=2:
        dt=max(tss)-min(tss); h,rem=divmod(int(dt.total_seconds()),3600); m=rem//60
        dur=f"{h}h {m}m"
    return dict(
        total=n, settled=ns, wins=nw, losses=nl, pending=len(pending),
        win_rate=wr, total_pnl=total_pnl, avg_win=avg_w, avg_loss=avg_l,
        profit_factor=pf, best_streak=best, worst_streak=worst,
        latest_streak=lstreak, latest_type=ltype, max_drawdown=dd,
        long_count=len(longs), short_count=len(shorts), long_wr=lwr, short_wr=swr,
        avg_ml_edge=aedge, avg_ml_pup=apup,
        avg_score=sum(t.get("signal_score",0) for t in trades)/n if n else 0,
        avg_conf=sum(t.get("signal_confidence",0) for t in trades)/n if n else 0,
        regime_stats=dict(rs), session_duration=dur,
    )

def C(text, code):
    if sys.platform=="win32":
        try:
            import colorama; colorama.just_fix_windows_console()
        except: return str(text)
    return f"\033[{code}m{text}\033[0m"
def G(t): return C(t,"32")
def R(t): return C(t,"31")
def Y(t): return C(t,"33")
def B(t): return C(t,"1")
def CY(t):return C(t,"36")

def print_summary(s):
    if not s:
        print("\nNo trades yet. Start the bot with:  python run_bot.py --test-mode\n")
        return
    w=s["win_rate"]; pnl=s["total_pnl"]
    pnl_s = G(f"${pnl:+.4f}") if pnl>=0 else R(f"${pnl:+.4f}")
    wr_s  = G(f"{w:.1f}%")    if w>=55   else (Y(f"{w:.1f}%") if w>=45 else R(f"{w:.1f}%"))
    pf    = s["profit_factor"]
    pf_s  = G(f"{pf:.2f}")    if pf>1.5  else (Y(f"{pf:.2f}") if pf>1.0 else R(f"{pf:.2f}"))
    dd    = s["max_drawdown"]
    dd_s  = G(f"${dd:.4f}")   if dd<0.05 else (Y(f"${dd:.4f}") if dd<0.10 else R(f"${dd:.4f}"))
    lt=s["latest_type"] or ""; ls=s["latest_streak"]
    st_s  = G(f"{ls}x WIN")   if lt=="WIN" else R(f"{ls}x LOSS") if lt else "N/A"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print(B("=" * 68))
    print(B(f"  SIMULATION RESULTS  [as of {now}]"))
    print(B("=" * 68))
    print()
    print(B("  OVERVIEW"))
    print(f"    Total trades:        {s['total']}")
    print(f"    Settled:             {s['settled']}  ({s['wins']} wins / {s['losses']} losses)")
    print(f"    Pending:             {s['pending']}")
    print(f"    Win rate:            {wr_s}")
    print(f"    Session duration:    {s['session_duration'] or 'N/A'}")
    print()
    print(B("  PROFITABILITY"))
    print(f"    Cumulative PnL:      {pnl_s}")
    aw = f"${s['avg_win']:+.4f}";  print(f"    Avg win:             {G(aw)}")
    al = f"${s['avg_loss']:+.4f}"; print(f"    Avg loss:            {R(al)}")
    print(f"    Profit factor:       {pf_s}  (>1.5 = good)")
    print(f"    Max drawdown:        {dd_s}")
    print()
    print(B("  STREAKS"))
    print(f"    Current:             {st_s}")
    print(f"    Best win streak:     {G(s['best_streak'])}")
    print(f"    Worst loss streak:   {R(s['worst_streak'])}")
    print()
    print(B("  DIRECTION"))
    print(f"    LONG  trades:  {s['long_count']:<4}  win rate: {s['long_wr']:.1f}%")
    print(f"    SHORT trades:  {s['short_count']:<4}  win rate: {s['short_wr']:.1f}%")
    print()
    print(B("  SIGNAL QUALITY"))
    print(f"    Avg signal score:    {s['avg_score']:.1f}")
    print(f"    Avg confidence:      {s['avg_conf']:.1%}")
    print(f"    Avg ML edge:         {s['avg_ml_edge']:.4f}")
    print(f"    Avg ML p(UP):        {s['avg_ml_pup']:.3f}")
    print()
    if s["regime_stats"]:
        print(B("  VOLATILITY REGIME BREAKDOWN"))
        for reg, d in sorted(s["regime_stats"].items()):
            n2=d["total"]; wr2=d["wins"]/n2*100 if n2 else 0
            bar = G("#")*d["wins"] + R("."*(n2-d["wins"]))
            print(f"    {reg:<14} {n2:>3} trades  {wr2:>5.1f}%  [{bar}]")
        print()
    print(B("=" * 68))
    print(Y("  NOTE: SIMULATION ONLY — no real money involved"))
    print(B("=" * 68))
    print()

def print_trade_table(trades, limit=None):
    disp = trades[-limit:] if limit else trades
    if not disp: return
    print(B(f"  TRADE LOG  ({len(disp)} shown of {len(trades)} total)"))
    sep = "-" * 112
    print(sep)
    print(B(f"  {'#':<5} {'Time':<17} {'Dir':<6} {'Entry':>7} {'Exit':>7} {'PnL':>9} {'ML p↑':>7} {'Edge':>6} {'Score':>6} {'Regime':<10} {'Outcome'}"))
    print(sep)
    for t in disp:
        ts  = datetime.fromisoformat(t["timestamp"]).strftime("%m-%d %H:%M:%S")
        num = t.get("session_trade_num","?")
        d   = t.get("direction","?")
        en  = t.get("entry_price",0.0)
        ex  = t.get("exit_price",0.0)
        p   = t.get("pnl_usd",0.0)
        mpu = t.get("ml_p_up",0.0)
        edg = t.get("ml_edge",0.0)
        sc  = t.get("signal_score",0.0)
        reg = (t.get("vol_regime") or "?")[:10]
        out = t.get("outcome","PENDING")
        ps  = G(f"${p:+.4f}") if p>=0 else R(f"${p:+.4f}")
        os_ = G("WIN") if out=="WIN" else (R("LOSS") if out=="LOSS" else Y("PENDING"))
        ds_ = CY(f"{d:<6}")
        print(f"  {str(num):<5} {ts:<17} {ds_} {en:>7.4f} {ex:>7.4f} {ps:>9} {mpu:>7.3f} {edg:>6.4f} {sc:>6.1f} {reg:<10} {os_}")
    print(sep)
    print()

def export_csv(trades, path="paper_trades.csv"):
    if not trades:
        print("No trades to export."); return
    import csv
    keys=["trade_id","timestamp","direction","size_usd","entry_price","exit_price",
          "pnl_usd","pnl_pct","outcome","signal_score","signal_confidence","num_signals",
          "ml_p_up","ml_edge","market_slug","btc_spot_price","vol_regime","funding_rate","session_trade_num"]
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(trades)
    print(f"Exported {len(trades)} trades to {path}")

def main():
    parser=argparse.ArgumentParser(description="View paper/simulation trade results")
    parser.add_argument("--last",    type=int,  metavar="N", help="Show only last N trades")
    parser.add_argument("--summary", action="store_true",    help="Summary stats only (no trade table)")
    parser.add_argument("--csv",     action="store_true",    help="Export to paper_trades.csv")
    parser.add_argument("--watch",   action="store_true",    help="Auto-refresh every 10 seconds")
    parser.add_argument("--file",    type=str,  default=str(TRADES_FILE))
    args=parser.parse_args()
    path=Path(args.file)

    def render():
        if args.watch:
            os.system("cls" if sys.platform=="win32" else "clear")
        trades=load_trades(path); s=compute_stats(trades)
        print_summary(s)
        if not args.summary:
            print_trade_table(trades, limit=args.last)
        if args.csv:
            export_csv(trades)
        if args.watch:
            print(CY("  [Auto-refresh every 10s  --  Ctrl+C to stop]"))

    if args.watch:
        try:
            while True:
                render(); time.sleep(10)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        render()

if __name__=="__main__":
    main()