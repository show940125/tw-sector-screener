from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


def render_dashboard(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data_json = json.dumps(payload, ensure_ascii=False)
    content = f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TW Sector Simulator - {html.escape(str(payload.get('run_id', '')))}</title>
  <style>
    body {{ margin: 0; font-family: Segoe UI, Noto Sans TC, Arial, sans-serif; color: #1f2937; background: #f7f8fb; }}
    header {{ padding: 24px 32px; background: #ffffff; border-bottom: 1px solid #d9dee8; }}
    main {{ padding: 24px 32px 48px; }}
    h1 {{ margin: 0 0 6px; font-size: 26px; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    .muted {{ color: #667085; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .card {{ background: #fff; border: 1px solid #d9dee8; border-radius: 8px; padding: 16px; }}
    .metric {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
    .notice {{ margin-top: 10px; padding: 10px 12px; border: 1px solid #f6c177; background: #fff7e6; border-radius: 8px; color: #7a4b00; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee8; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #edf0f5; text-align: left; font-size: 13px; }}
    th {{ background: #f0f3f8; }}
    svg {{ width: 100%; height: 280px; background: #fff; border: 1px solid #d9dee8; border-radius: 8px; }}
    .warn {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <header>
    <h1>TW Sector Screener 投資模擬器</h1>
    <div class="muted">run: <span id="run-id"></span> ｜ themes: <span id="themes"></span> ｜ period: <span id="period"></span></div>
    <div id="market-status"></div>
  </header>
  <main>
    <section>
      <h2>三人格總覽</h2>
      <div id="summary" class="grid"></div>
    </section>
    <section>
      <h2>Equity Curve</h2>
      <svg id="equity-chart" viewBox="0 0 900 280" role="img" aria-label="equity curve"></svg>
    </section>
    <section>
      <h2>持倉明細</h2>
      <div id="positions"></div>
    </section>
    <section>
      <h2>今日成交委託</h2>
      <div id="orders"></div>
    </section>
    <section>
      <h2>明日計畫委託</h2>
      <div id="planned-orders"></div>
    </section>
    <section>
      <h2>Buying Ranking</h2>
      <div id="buying-ranking"></div>
    </section>
    <section>
      <h2>Actionable Queue</h2>
      <div id="actionable-queue"></div>
    </section>
  </main>
  <script>
    const DATA = {data_json};
    const fmt = new Intl.NumberFormat('zh-TW', {{ maximumFractionDigits: 0 }});
    document.getElementById('run-id').textContent = DATA.run_id;
    document.getElementById('themes').textContent = (DATA.themes || []).join(', ');
    document.getElementById('period').textContent = `${{DATA.start_date || ''}} ~ ${{DATA.end_date || ''}}`;
    const marketStatus = DATA.market_status || {{}};
    if (marketStatus.note) {{
      const div = document.getElementById('market-status');
      div.className = marketStatus.is_trading_day === false ? 'notice' : 'muted';
      div.textContent = marketStatus.note;
    }}
    const summary = document.getElementById('summary');
    for (const item of DATA.portfolio_summaries || []) {{
      const div = document.createElement('div');
      div.className = 'card';
      div.innerHTML = `<div>${{item.name}}</div><div class="metric">${{fmt.format(item.equity)}} 元</div>
        <div class="muted">報酬 ${{item.return_pct.toFixed(2)}}% ｜ 回撤 ${{item.max_drawdown_pct.toFixed(2)}}%</div>
        <div class="muted">現金 ${{fmt.format(item.cash)}} ｜ 持股 ${{fmt.format(item.holdings_value)}}</div>
        <div class="muted">VaR95 ${{((item.portfolio_diagnostics || {{}}).var95_pct || 0).toFixed(2)}}% ｜ CVaR95 ${{((item.portfolio_diagnostics || {{}}).cvar95_pct || 0).toFixed(2)}}% ｜ Omega ${{((item.portfolio_diagnostics || {{}}).omega_ratio || 0).toFixed(2)}}</div>`;
      summary.appendChild(div);
    }}
    function drawEquity() {{
      const svg = document.getElementById('equity-chart');
      const rows = DATA.daily_equity || [];
      const ids = [...new Set(rows.map(r => r.portfolio_id))];
      const values = rows.map(r => r.equity);
      const min = Math.min(...values, DATA.initial_cash || 0);
      const max = Math.max(...values, DATA.initial_cash || 1);
      const colors = ['#c2410c', '#2563eb', '#047857'];
      ids.forEach((id, idx) => {{
        const series = rows.filter(r => r.portfolio_id === id);
        const points = series.map((r, i) => {{
          const x = 40 + (i * 820 / Math.max(series.length - 1, 1));
          const y = 240 - ((r.equity - min) * 200 / Math.max(max - min, 1));
          return `${{x}},${{y}}`;
        }}).join(' ');
        const poly = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
        poly.setAttribute('points', points);
        poly.setAttribute('fill', 'none');
        poly.setAttribute('stroke', colors[idx % colors.length]);
        poly.setAttribute('stroke-width', '3');
        svg.appendChild(poly);
      }});
    }}
    function table(el, rows, cols) {{
      if (!rows.length) {{ el.textContent = '無資料'; return; }}
      const html = `<table><thead><tr>${{cols.map(c => `<th>${{c[1]}}</th>`).join('')}}</tr></thead><tbody>` +
        rows.map(r => `<tr>${{cols.map(c => `<td>${{r[c[0]] ?? ''}}</td>`).join('')}}</tr>`).join('') + '</tbody></table>';
      el.innerHTML = html;
    }}
    drawEquity();
    table(document.getElementById('positions'), DATA.positions || [], [['portfolio_id','Portfolio'], ['symbol','代碼'], ['name','名稱'], ['quantity','股數'], ['avg_cost','成本'], ['last_price','現價'], ['unrealized_pct','未實現%'], ['recommendation','建議'], ['risk_score','Risk']]);
    table(document.getElementById('orders'), (DATA.orders || []).slice(-80), [['date','日期'], ['portfolio_id','Portfolio'], ['symbol','代碼'], ['side','方向'], ['quantity','股數'], ['limit_price','限價'], ['fill_price','成交價'], ['status','狀態'], ['reason','理由'], ['policy_violation','違規']]);
    table(document.getElementById('planned-orders'), (DATA.planned_orders || []).slice(-80), [['date','日期'], ['portfolio_id','Portfolio'], ['symbol','代碼'], ['side','方向'], ['quantity','股數'], ['order_type','委託'], ['status','狀態'], ['reason','理由']]);
    table(document.getElementById('buying-ranking'), DATA.buying_ranking || [], [['theme','題材'], ['list_rank','List'], ['rank','Rank'], ['symbol','代碼'], ['name','名稱'], ['recommendation','建議'], ['buying_tier','Buying Tier'], ['decision_tier','Decision'], ['buyability_score','Buyability'], ['confidence_score','Confidence'], ['risk_score','Risk'], ['close','收盤'], ['trigger_to_upgrade','觸發']]);
    table(document.getElementById('actionable-queue'), DATA.actionable_queue || [], [['theme','題材'], ['list_rank','List'], ['rank','Rank'], ['symbol','代碼'], ['name','名稱'], ['recommendation','建議'], ['buying_tier','Buying Tier'], ['decision_tier','Decision'], ['actionability_score','Actionability'], ['confidence_score','Confidence'], ['risk_score','Risk'], ['close','收盤'], ['next_action','下一步']]);
  </script>
</body>
</html>
"""
    path.write_text(content, encoding="utf-8")
    return path
