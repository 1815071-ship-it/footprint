// Footprint-кластеры для панели Business Charts (Apache ECharts) в Grafana.
// Этот файл - исходник. В дашборд он попадает через build_dashboard.py (поле options.getOption).
//
// Данные приходят двумя запросами:
//   A - кластеры: time (начало бара), level (нижняя граница уровня цены), sell, buy (объём в монетах)
//   B - свечи:    time, open, high, low, close
// На каждый бар рисуем слот: свеча слева, дальше три колонки по уровням цены:
//   Sell (агрессивные продажи) | Buy (агрессивные покупки) | Delta = Buy - Sell с полосой.
// Рамкой выделен уровень с максимальным объёмом в баре (POC).

const theme = context.grafana.theme;
const dark = theme.isDark;
const TEXT = theme.colors.text.primary;
const MUTED = theme.colors.text.secondary;
const GRID = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
const RED = dark ? '#ff5c5c' : '#d93025';
const GREEN = dark ? '#3ddc84' : '#188038';
const RED_BAR = dark ? 'rgba(255,92,92,0.55)' : 'rgba(217,48,37,0.45)';
const GREEN_BAR = dark ? 'rgba(61,220,132,0.55)' : 'rgba(24,128,56,0.45)';
const POC = dark ? 'rgba(255,214,10,0.9)' : 'rgba(176,128,0,0.9)';

const v = (name) => context.grafana.replaceVariables(name);
const step = Number(v('$step')) || 50;
const sym = v('$symbol');
const frames = context.panel.data.series || [];
const frame = (ref) => frames.find((f) => f.refId === ref);
const col = (f, name) => {
  const fld = f && f.fields.find((x) => x.name === name);
  return fld ? Array.from(fld.values) : [];
};

const A = frame('A');
const B = frame('B');
const at = col(A, 'time'), lv = col(A, 'level'), sl = col(A, 'sell'), by = col(A, 'buy');
const bt = col(B, 'time'), bo = col(B, 'open'), bh = col(B, 'high'), bl = col(B, 'low'), bc = col(B, 'close');

// Слоты = бары по времени. Слот i занимает по оси X отрезок [i, i+1).
const times = Array.from(new Set([...at, ...bt])).sort((a, b) => a - b);
const slotOf = new Map(times.map((t, i) => [t, i]));
const n = times.length;

const pad = (x) => String(x).padStart(2, '0');
const label = (ms) => {
  const d = new Date(ms);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
};
const fmt = (x) => {
  const a = Math.abs(x);
  if (a >= 10000) return (x / 1000).toFixed(1) + 'k';
  if (a >= 100) return x.toFixed(0);
  if (a >= 10) return x.toFixed(1);
  return x.toFixed(2);
};

// Ячейки: [слот, уровень, sell, buy, delta]. Плюс максимумы по слоту для масштаба полос и POC.
const cells = [];
const maxDelta = new Array(n).fill(0);
const pocVol = new Array(n).fill(-1);
const pocLevel = new Array(n).fill(null);
for (let i = 0; i < at.length; i++) {
  const s = slotOf.get(at[i]);
  const sell = sl[i] || 0, buy = by[i] || 0, d = buy - sell;
  cells.push([s, lv[i], sell, buy, d]);
  maxDelta[s] = Math.max(maxDelta[s], Math.abs(d));
  if (sell + buy > pocVol[s]) {
    pocVol[s] = sell + buy;
    pocLevel[s] = lv[i];
  }
}
const candles = bt.map((t, i) => [slotOf.get(t), bo[i], bh[i], bl[i], bc[i]]);

// Окно просмотра: при обновлении данных сохраняем масштаб; если смотрели последние бары - едем за ними.
const chart = context.panel.chart || null;
const key = [sym, v('$tf'), step, v('$market'), v('$exchange')].join('|');
const perSlot = 190;
const width = chart && chart.getWidth ? chart.getWidth() : 1400;
const fit = Math.max(3, Math.floor((width - 80) / perSlot));
let startValue = Math.max(0, n - fit), endValue = n;
const prev = chart && chart.__fp;
if (prev && prev.key === key && chart.getOption) {
  const z = ((chart.getOption() || {}).dataZoom || [])[0];
  if (z && z.startValue != null && z.endValue != null) {
    const width = z.endValue - z.startValue;
    if (z.endValue >= prev.n - 0.5) {
      endValue = n;
      startValue = Math.max(0, n - width);
    } else {
      startValue = z.startValue;
      endValue = z.endValue;
    }
  }
}
if (chart) chart.__fp = { key, n };

const clusterSeries = {
  type: 'custom',
  name: 'clusters',
  data: cells,
  encode: { x: 0, y: 1 },
  clip: true,
  renderItem: (params, api) => {
    const s = api.value(0), level = api.value(1), sell = api.value(2), buy = api.value(3), d = api.value(4);
    const top = api.coord([s, level + step])[1];
    const bot = api.coord([s, level])[1];
    const h = Math.abs(bot - top);
    const x0 = api.coord([s, level])[0];
    const w = api.size([1, 0])[0];
    const kids = [];
    // полоса дельты: длина относительно максимальной дельты в этом баре
    const bw = maxDelta[s] > 0 ? (Math.abs(d) / maxDelta[s]) * w * 0.3 : 0;
    kids.push({
      type: 'rect',
      shape: { x: x0 + w * 0.66, y: top + h * 0.1, width: Math.max(bw, 1), height: h * 0.8 },
      style: { fill: d >= 0 ? GREEN_BAR : RED_BAR },
    });
    if (pocLevel[s] === level) {
      kids.push({
        type: 'rect',
        shape: { x: x0 + w * 0.17, y: top + 0.5, width: w * 0.47, height: Math.max(h - 1, 1) },
        style: { fill: 'none', stroke: POC, lineWidth: 1 },
      });
    }
    // цифры рисуем, только когда ячейка достаточно высокая и бар достаточно широкий
    if (h >= 8 && w >= 110) {
      const fs = Math.max(8, Math.min(12, Math.floor(h * 0.75)));
      const yMid = top + h / 2;
      const txt = (x, value, color, align, bold) => ({
        type: 'text',
        style: {
          x, y: yMid, text: fmt(value), fill: color, align, verticalAlign: 'middle',
          font: `${bold ? '600 ' : ''}${fs}px sans-serif`,
        },
      });
      kids.push(txt(x0 + w * 0.39, sell, RED, 'right'));
      kids.push(txt(x0 + w * 0.62, buy, GREEN, 'right'));
      kids.push(txt(x0 + w * 0.67, d, TEXT, 'left', true));
    }
    return { type: 'group', children: kids };
  },
};

const candleSeries = {
  type: 'custom',
  name: 'candles',
  data: candles,
  encode: { x: 0, y: [1, 2, 3, 4] },
  clip: true,
  z: 3,
  renderItem: (params, api) => {
    const s = api.value(0), o = api.value(1), hi = api.value(2), lo = api.value(3), c = api.value(4);
    const w = api.size([1, 0])[0];
    const x0 = api.coord([s, o])[0];
    const xc = x0 + w * 0.085;
    const yo = api.coord([s, o])[1], yc = api.coord([s, c])[1];
    const yh = api.coord([s, hi])[1], yl = api.coord([s, lo])[1];
    const color = c >= o ? GREEN : RED;
    return {
      type: 'group',
      children: [
        { type: 'line', shape: { x1: xc, y1: yh, x2: xc, y2: yl }, style: { stroke: color, lineWidth: 1 } },
        {
          type: 'rect',
          shape: { x: x0 + w * 0.03, y: Math.min(yo, yc), width: w * 0.11, height: Math.max(Math.abs(yc - yo), 1) },
          style: { fill: color },
        },
      ],
    };
  },
};

return {
  backgroundColor: 'transparent',
  animation: false,
  grid: { left: 8, right: 72, top: 10, bottom: 46 },
  tooltip: {
    trigger: 'item',
    confine: true,
    formatter: (p) => {
      const d = p.data;
      const when = label(times[d[0]]);
      if (p.seriesName === 'candles') {
        return `${when} UTC<br/>open ${d[1]}<br/>high ${d[2]}<br/>low ${d[3]}<br/>close ${d[4]}`;
      }
      return `${when} UTC · $${d[1]}–${d[1] + step}<br/>` +
        `<span style="color:${RED}">Sell ${fmt(d[2])} ${sym}</span><br/>` +
        `<span style="color:${GREEN}">Buy ${fmt(d[3])} ${sym}</span><br/>` +
        `<b>Delta ${fmt(d[4])} ${sym}</b>`;
    },
  },
  xAxis: {
    type: 'value',
    min: 0,
    max: n,
    interval: 1,
    axisLine: { lineStyle: { color: GRID } },
    axisTick: { show: false },
    splitLine: { show: true, lineStyle: { color: GRID } },
    axisLabel: { color: MUTED, formatter: (x) => (Number.isInteger(x) && x < n ? label(times[x]) : '') },
  },
  yAxis: {
    type: 'value',
    position: 'right',
    scale: true,
    max: (e) => Math.ceil(e.max / step) * step + step,
    min: (e) => Math.floor(e.min / step) * step,
    splitLine: { show: true, lineStyle: { color: GRID } },
    axisLabel: { color: MUTED, formatter: (x) => '$' + x },
  },
  dataZoom: [
    { type: 'inside', xAxisIndex: 0, filterMode: 'weakFilter', startValue, endValue, minValueSpan: 1 },
    {
      type: 'slider', xAxisIndex: 0, filterMode: 'weakFilter', startValue, endValue, height: 14, bottom: 6,
      showDetail: false, borderColor: GRID, textStyle: { color: MUTED },
    },
    { type: 'inside', yAxisIndex: 0, filterMode: 'none', zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift' },
  ],
  series: [clusterSeries, candleSeries],
};
