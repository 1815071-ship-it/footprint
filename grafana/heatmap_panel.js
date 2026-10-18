// Тепловая карта стакана для панели Business Charts (Apache ECharts) в Grafana.
// Исходник; в дашборд попадает через build_dashboard.py (поле options.getOption).
//
// Данные:
//   H - ячейки: time (начало отрезка), level (нижняя граница уровня), usd (сколько $ в среднем стояло
//       в заявках на уровне за минуту, сумма по выбранным стаканам и обеим сторонам), step ($ на уровень),
//       sec (ширина отрезка, секунд) - step и sec одинаковые во всех строках
//   L - цена: time, close, high, low по тем же отрезкам (Binance perp для «все»)
//   T - крупные рыночные заявки (tr-1): time (отрезок), buy (1 - покупки, 0 - продажи), price (средняя цена,
//       взвешенная по $), usd (сумма), n (число заявок), src (биржи) - по ячейке карты и стороне
// Как читать вместе: красные кружки бьют в яркую полосу покупателей, а цена стоит - стену поглощают;
// полоса пропала до подхода цены, а сделок на ней не было - заявку сняли (спуфинг).
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
const BUY = dark ? '#3ddc84' : '#188038';
const SELL = dark ? '#ff5c5c' : '#d93025';
const RING = dark ? '#111217' : '#ffffff';   // обводка кружков цветом фона: отделяет их от карты и друг от друга
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
const T = frame('T');
const tt = col(T, 'time'), tb = col(T, 'buy'), tp = col(T, 'price'), tu = col(T, 'usd'), tn = col(T, 'n'),
  ts = col(T, 'src');

// Два режима цвета (переменная $heat):
//   rel (по умолчанию) - «стены»: во сколько раз на уровне стоит больше, чем обычно стоит на таком же
//       расстоянии от цены за всё окно (медиана по всем отрезкам). У цены всегда много заявок (там видны все
//       6 стаканов), поэтому в объёмах облако у цены забивает всё; в отношении к обычному оно становится ровным
//       фоном, а стены - и у цены, и вдали - выделяются. Шкала: обычно и меньше - тёмный, x8 и больше - самый яркий.
//   abs - сам объём $: корень от доли от 99-го перцентиля ячеек.
const mode = v('$heat') === 'abs' ? 'abs' : 'rel';
const RATIO_MAX = 8;
const closeAt = new Map(lt.map((t, i) => [t, lc[i]]));
let lastClose = lc.length ? lc[0] : null;
const dist = ht.map((t, i) => {          // расстояние уровня от цены закрытия отрезка, в строках карты
  const c = closeAt.has(t) ? closeAt.get(t) : lastClose;
  if (closeAt.has(t)) lastClose = c;
  return c == null ? 0 : Math.round((hl[i] + step / 2 - c) / step);
});
const median = (arr) => {
  const a = arr.slice().sort((x, y) => x - y);
  return a.length ? a[Math.floor(a.length / 2)] : 0;
};
const byDist = new Map();
dist.forEach((d, i) => { if (!byDist.has(d)) byDist.set(d, []); byDist.get(d).push(hu[i]); });
const allMed = median(hu) || 1;
const baseCache = new Map();
const baseline = (d) => {                // обычный объём на расстоянии d; мало данных - соседние строки, потом общая
  if (baseCache.has(d)) return baseCache.get(d);
  let vals = byDist.get(d) || [];
  for (let k = 1; vals.length < 20 && k <= 3; k++) vals = vals.concat(byDist.get(d - k) || [], byDist.get(d + k) || []);
  const b = vals.length >= 10 ? median(vals) : allMed;
  baseCache.set(d, b || allMed);
  return b || allMed;
};
const ratio = hu.map((u, i) => u / baseline(dist[i]));
// Стена - это то, что стоит долго. Одиночные вспышки на одну-две минуты (заявку поставили и сняли, или цена
// пробежала уровень внутри отрезка) гасим: цвет берём по медиане отношения за 5 соседних отрезков на том же
// уровне (пустой отрезок = 0; у краёв окна - только существующие отрезки).
const nCols = Math.round((Math.max(...ht, 0) - Math.min(...ht, 0)) / secMs) + 1;
const t0 = ht.length ? Math.min(...ht) : 0;
const colOf = (t) => Math.round((t - t0) / secMs);
const atLevel = new Map();                 // уровень -> Map(отрезок -> отношение)
ht.forEach((t, i) => {
  if (!atLevel.has(hl[i])) atLevel.set(hl[i], new Map());
  atLevel.get(hl[i]).set(colOf(t), ratio[i]);
});
const smooth = ht.map((t, i) => {
  const row = atLevel.get(hl[i]), c = colOf(t), w = [];
  for (let k = Math.max(0, c - 2); k <= Math.min(nCols - 1, c + 2); k++) w.push(row.get(k) || 0);
  return median(w);
});
const sorted = hu.filter((x) => x > 0).sort((a, b) => a - b);
const vmax = sorted.length ? sorted[Math.floor((sorted.length - 1) * 0.99)] : 1;
const binOf = (i) => (mode === 'abs'
  ? Math.min(NB - 1, Math.floor(Math.sqrt(Math.min(1, hu[i] / vmax)) * NB))
  : Math.min(NB - 1, Math.max(0, Math.floor((Math.log(Math.max(smooth[i], 1)) / Math.log(RATIO_MAX)) * NB))));
const cells = ht.map((t, i) => [t, hl[i], hu[i], binOf(i), ratio[i]]);

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

// Крупные сделки: площадь кружка пропорциональна сумме, самый крупный в окне - 28 px в диаметре.
// Продажи чуть левее середины отрезка, покупки чуть правее: в одной ячейке не закрывают друг друга.
const umax = tu.length ? Math.max(...tu) : 1;
const bubble = (usd) => 5 + 23 * Math.sqrt(usd / umax);
const trades = (side) => tt.map((t, i) => [t + secMs * (side ? 0.62 : 0.38), tp[i], tu[i], tn[i], ts[i], tb[i]])
  .filter((d) => d[5] === side);

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
const fmtX = (r) => (r >= 10 ? Math.round(r) : r.toFixed(1)) + ' раза';
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

const tradeSeries = (side) => ({
  type: 'scatter',
  name: side ? 'buys' : 'sells',
  data: trades(side),
  symbolSize: (d) => bubble(d[2]),
  itemStyle: { color: side ? BUY : SELL, opacity: 0.9, borderColor: RING, borderWidth: 1.5 },
  emphasis: { scale: 1.2 },
  z: 6,
});

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
    style: { text: mode === 'abs' ? '$0' : 'как обычно', fill: MUTED, font: '11px sans-serif' } });
  legend.push({ type: 'text', right: 72, top: 4, z: 6,
    style: { text: mode === 'abs' ? '≥ ' + fmtUsd(vmax) : '×' + RATIO_MAX + ' и больше', fill: MUTED,
      font: '11px sans-serif' } });
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
      const d = p.data;
      if (p.seriesName === 'buys' || p.seriesName === 'sells') {
        const t0 = d[0] - secMs * (d[5] ? 0.62 : 0.38);
        return `${tLabel(t0)}–${hhmm(t0 + secMs)} UTC<br/>` +
          `<b style="color:${d[5] ? BUY : SELL}">${d[5] ? 'Покупки' : 'Продажи'} по рынку ${fmtUsd(d[2])}</b>` +
          ` · ${d[3]} ${d[3] % 10 === 1 && d[3] % 100 !== 11 ? 'заявка' : (d[3] % 10 >= 2 && d[3] % 10 <= 4 && (d[3] % 100 < 10 || d[3] % 100 >= 20) ? 'заявки' : 'заявок')}<br/>` +
          `средняя цена $${Math.round(d[1] * 100) / 100}<br/>${d[4]}`;
      }
      if (p.seriesName !== 'heat') return '';
      return `${tLabel(d[0])}–${hhmm(d[0] + secMs)} UTC · $${d[1]}–${d[1] + step}<br/>` +
        `<b>${fmtUsd(d[2])}</b> в заявках (в среднем за минуту)<br/>` +
        (d[4] >= 1.1 ? `в ${fmtX(d[4])} больше обычного на таком расстоянии от цены`
          : d[4] <= 0.9 ? `в ${fmtX(1 / d[4])} меньше обычного на таком расстоянии от цены`
            : 'как обычно на таком расстоянии от цены');
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
  series: [heatSeries, priceSeries, tradeSeries(0), tradeSeries(1)],
};
