// Тепловая карта стакана для панели Business Charts (Apache ECharts) в Grafana.
// Исходник; в дашборд попадает через build_dashboard.py (поле options.getOption).
//
// Данные:
//   H - ячейки: time (начало отрезка), level (нижняя граница уровня), usd (сколько $ в среднем стояло
//       в заявках на уровне за минуту, сумма по выбранным стаканам и обеим сторонам), step ($ на уровень),
//       sec (ширина отрезка, секунд) - step и sec одинаковые во всех строках
//   L - цена: time, close, high, low по тем же отрезкам (Binance perp для «все»)
// Цвет - один оттенок синего: тёмный = мало, светлый = много (в светлой теме наоборот). Шкала - корень
// от доли от 99-го перцентиля: крупные стены не сжимают остальное в одну краску; всё выше перцентиля - самый
// яркий цвет. Длинная яркая полоса - заявка, которая долго стояла на одном уровне.
// Оговорка: глубина стаканов разная (BTC: Binance perp и Bybit spot ±0.15%, Bybit perp ±0.24%, OKX perp ±0.85%,
// Binance spot ±1.05%, OKX spot ±1.2% от цены), поэтому у цены складываются все 6 стаканов, а дальше - только
// глубокие. Яркая полоса вдоль цены отчасти из-за этого; стены дальше ±0.25% - это OKX и Binance spot.

const theme = context.grafana.theme;
const dark = theme.isDark;
const MUTED = theme.colors.text.secondary;
const GRID = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
const PRICE = dark ? '#ffd60a' : '#b08000';
// синяя шкала (шаги 700 -> 100): от фона к яркому в тёмной теме, в светлой - наоборот
const BLUE = ['#0d366b', '#104281', '#184f95', '#1c5cab', '#256abf', '#2a78d6', '#3987e5', '#5598e7', '#6da7ec',
  '#86b6ef', '#9ec5f4', '#b7d3f6', '#cde2fb'];
const RAMP = dark ? BLUE : BLUE.slice().reverse();
const NB = RAMP.length;

const v = (name) => context.grafana.replaceVariables(name);
const sym = v('$symbol');
const exch = v('$exchange');
const frames = context.panel.data.series || [];
const frame = (ref) => frames.find((f) => f.refId === ref);
const col = (f, name) => {
  const fld = f && f.fields.find((x) => x.name === name);
  return fld ? Array.from(fld.values) : [];
};

const H = frame('H');
const L = frame('L');
const ht = col(H, 'time'), hl = col(H, 'level'), hu = col(H, 'usd');
const step = Number(col(H, 'step')[0]) || (sym === 'BTC' ? 10 : 1);
const secMs = (Number(col(H, 'sec')[0] || col(L, 'sec')[0]) || 60) * 1000;
const lt = col(L, 'time'), lc = col(L, 'close'), lh = col(L, 'high'), ll = col(L, 'low');

// Масштаб цвета: 99-й перцентиль ячеек.
const sorted = hu.filter((x) => x > 0).sort((a, b) => a - b);
const vmax = sorted.length ? sorted[Math.floor((sorted.length - 1) * 0.99)] : 1;
const binOf = (usd) => Math.min(NB - 1, Math.floor(Math.sqrt(Math.min(1, usd / vmax)) * NB));
const cells = ht.map((t, i) => [t, hl[i], hu[i], binOf(hu[i])]);

// Шкала цены: диапазон свечей с запасом (как в SQL, build_dashboard.py SQL_HEAT).
const lo = ll.length ? Math.min(...ll) : (hl.length ? Math.min(...hl) : 0);
const hi = lh.length ? Math.max(...lh) : (hl.length ? Math.max(...hl) + step : 1);
const pad = Math.max((hi - lo) * 0.3, hi * 0.0025);
const yMin = Math.floor((lo - pad) / step) * step;
const yMax = Math.ceil((hi + pad) / step) * step;

const times = ht.concat(lt);
const xMin = times.length ? Math.min(...times) : Date.now() - 3 * 3600e3;
const xMax = times.length ? Math.max(...times) + secMs : Date.now();
const priceLine = lt.map((t, i) => [t + secMs / 2, lc[i]]);

const pad2 = (x) => String(x).padStart(2, '0');
const hhmm = (ms) => {
  const d = new Date(ms);
  return `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
};
const longSpan = xMax - xMin > 26 * 3600e3;
const tLabel = (ms) => {
  const d = new Date(ms);
  return longSpan ? `${pad2(d.getUTCDate())}.${pad2(d.getUTCMonth() + 1)} ${hhmm(ms)}` : hhmm(ms);
};
const fmtUsd = (x) => (x >= 1e6 ? '$' + (x / 1e6).toFixed(1) + 'M' : '$' + Math.round(x / 1e3) + 'k');

// Окно просмотра: при обновлении сохраняем масштаб; если смотрели правый край - едем за ним.
const chart = context.panel.chart || null;
const key = [sym, v('$market'), exch].join('|');
let startValue = xMin, endValue = xMax;
const prev = chart && chart.__heat;
if (prev && prev.key === key && chart.getOption) {
  const z = ((chart.getOption() || {}).dataZoom || [])[0];
  if (z && z.startValue != null && z.endValue != null && (z.startValue > prev.xMin || z.endValue < prev.xMax)) {
    const width = z.endValue - z.startValue;
    if (z.endValue >= prev.xMax - secMs / 2) {
      endValue = xMax;
      startValue = Math.max(xMin, xMax - width);
    } else {
      startValue = z.startValue;
      endValue = z.endValue;
    }
  }
}
if (chart) chart.__heat = { key, xMin, xMax };

const heatSeries = {
  type: 'custom',
  name: 'heat',
  data: cells,
  encode: { x: 0, y: 1 },
  clip: true,
  progressive: 0,
  renderItem: (params, api) => {
    const t = api.value(0), level = api.value(1), bin = api.value(3);
    // углы ячейки округляем до пикселя: соседние ячейки стыкуются без светлых швов
    const a = api.coord([t, level + step]);
    const b = api.coord([t + secMs, level]);
    const x0 = Math.round(a[0]), y0 = Math.round(a[1]);
    return {
      type: 'rect',
      shape: { x: x0, y: y0, width: Math.max(Math.round(b[0]) - x0, 1), height: Math.max(Math.round(b[1]) - y0, 1) },
      style: { fill: RAMP[bin] },
    };
  },
};

const priceSeries = {
  type: 'line',
  name: 'price',
  data: priceLine,
  showSymbol: false,
  symbol: 'none',
  lineStyle: { color: PRICE, width: 1.5 },
  z: 5,
  silent: true,
};

// Легенда цвета: полоска из шагов шкалы и подписи «0» и «≥ 99-й перцентиль».
const LEG_W = 10, LEG_H = 8;
const legend = [];
if (cells.length) {
  RAMP.forEach((c, i) => legend.push({
    type: 'rect', right: 72 + 60 + (NB - 1 - i) * LEG_W, top: 6, z: 6,
    shape: { width: LEG_W, height: LEG_H }, style: { fill: c },
  }));
  legend.push({ type: 'text', right: 72 + 64 + NB * LEG_W, top: 4, z: 6,
    style: { text: '$0', fill: MUTED, font: '11px sans-serif' } });
  legend.push({ type: 'text', right: 72, top: 4, z: 6,
    style: { text: '≥ ' + fmtUsd(vmax), fill: MUTED, font: '11px sans-serif' } });
} else {
  legend.push({ type: 'text', left: 'center', top: 'middle', z: 6,
    style: {
      text: exch === 'coinbase' ? 'Стакан Coinbase не собираем' : 'Нет снимков стакана за этот период',
      fill: MUTED, font: '13px sans-serif',
    } });
}

return {
  backgroundColor: 'transparent',
  animation: false,
  // фон области графика - нижний цвет шкалы: уровень без строки в базе = там стояло меньше порога хранения
  grid: { left: 8, right: 72, top: 22, bottom: 46, show: true, backgroundColor: RAMP[0], borderWidth: 0 },
  tooltip: {
    trigger: 'item',
    confine: true,
    formatter: (p) => {
      if (p.seriesName !== 'heat') return '';
      const d = p.data;
      return `${tLabel(d[0])}–${hhmm(d[0] + secMs)} UTC · $${d[1]}–${d[1] + step}<br/>` +
        `<b>${fmtUsd(d[2])}</b> в заявках (в среднем за минуту)`;
    },
  },
  xAxis: {
    type: 'time',
    min: xMin,
    max: xMax,
    axisLine: { lineStyle: { color: GRID } },
    splitLine: { show: false },
    axisLabel: { color: MUTED, formatter: (x) => tLabel(x), hideOverlap: true },
  },
  yAxis: {
    type: 'value',
    position: 'right',
    min: yMin,
    max: yMax,
    splitLine: { show: true, lineStyle: { color: GRID } },
    axisLabel: { color: MUTED, formatter: (x) => '$' + x },
  },
  dataZoom: [
    { type: 'inside', xAxisIndex: 0, filterMode: 'weakFilter', startValue, endValue },
    {
      type: 'slider', xAxisIndex: 0, filterMode: 'weakFilter', startValue, endValue, height: 14, bottom: 6,
      showDetail: false, borderColor: GRID, textStyle: { color: MUTED },
    },
    { type: 'inside', yAxisIndex: 0, filterMode: 'none', zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift' },
  ],
  graphic: legend,
  series: [heatSeries, priceSeries],
};
