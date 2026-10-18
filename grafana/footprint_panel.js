// Footprint-кластеры для панели Business Charts (Apache ECharts) в Grafana.
// Этот файл - исходник. В дашборд он попадает через build_dashboard.py (поле options.getOption).
//
// Данные приходят тремя запросами:
//   A - кластеры: time (начало бара), level (нижняя граница уровня цены), sell, buy (объём в монетах)
//   B - свечи:    time, open, high, low, close
//   C - стакан сейчас: ask (1 - заявки на продажу, 0 - на покупку), level, usd (сумма заявок на уровне, $),
//       snap (время последнего снимка, одинаковое во всех строках)
// На каждый бар рисуем слот: свеча слева, дальше три колонки по уровням цены:
//   Sell (агрессивные продажи) | Buy (агрессивные покупки) | Delta = Buy - Sell с полосой.
// Рамкой выделен уровень с максимальным объёмом в баре (POC).
// Справа от графика, на той же шкале цены, - крупные заявки стакана в последнем снимке (как в CoinGlass):
// красные полосы продавцов, зелёные - покупателей, длина полосы - сумма заявок на уровне.

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
const ASK_BAR = dark ? 'rgba(255,92,92,0.75)' : 'rgba(217,48,37,0.7)';
const BID_BAR = dark ? 'rgba(61,220,132,0.75)' : 'rgba(24,128,56,0.7)';

const v = (name) => context.grafana.replaceVariables(name);
const sym = v('$symbol');
// «авто»: та же таблица, что в SQL (build_dashboard.py, STEP) - менять вместе
const autoStep = (s, tf) => (s === 'BTC'
  ? ({ '1 hour': 25, '4 hours': 100 }[tf] || 10)
  : ({ '1 hour': 2, '4 hours': 5 }[tf] || 1));
const step = v('$step') === 'auto' ? autoStep(sym, v('$tf')) : (Number(v('$step')) || 50);
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
const C = frame('C');
const ca = col(C, 'ask'), cl = col(C, 'level'), cu = col(C, 'usd');

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

// Стакан: [0, null, уровень, $, ask, обе стороны]. Ось Y привязана к пустому измерению 1, чтобы стакан
// (он шире по цене, чем свечи) не растягивал шкалу: шкала по-прежнему подстраивается под видимые бары.
// Длина полосы - доля от самого крупного уровня в видимой части шкалы. Если на уровне есть и покупатели,
// и продавцы (цены на биржах чуть расходятся), уровень делится пополам: сверху продавцы, снизу покупатели.
const sides = new Map();
cl.forEach((l, i) => sides.set(l, (sides.get(l) || 0) | (ca[i] ? 2 : 1)));
const book = cl.map((l, i) => [0, null, l, cu[i] || 0, ca[i] ? 1 : 0, sides.get(l) === 3 ? 1 : 0]);
const snapMs = col(C, 'snap')[0];
const BOOKW = book.length ? 150 : 0;   // ширина колонки стакана, px
const fmtUsd = (x) => (x >= 1e6 ? '$' + (x / 1e6).toFixed(1) + 'M' : '$' + Math.round(x / 1e3) + 'k');

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

// Колонка стакана рисуется справа от области графика, между свечами и шкалой цены.
// Своя скрытая ось X (xAxisIndex 1), чтобы прокрутка по времени её не фильтровала; ось Y общая.
const bookSeries = {
  type: 'custom',
  name: 'book',
  xAxisIndex: 1,
  data: book,
  encode: { x: 0, y: 1 },
  clip: false,
  z: 4,
  renderItem: (params, api) => {
    const cs = params.coordSys;
    const ctx = params.context;
    if (ctx.vmax == null) {
      // видимый диапазон цен: переводим края области графика из пикселей в цену (шкала линейная)
      const ya = api.coord([0, 0])[1], yb = api.coord([0, 100000])[1];
      const price = (y) => ((y - ya) * 100000) / (yb - ya);
      const lo = price(cs.y + cs.height), hi = price(cs.y);
      ctx.vmax = 0;
      for (const b of book) if (b[2] + step > lo && b[2] < hi) ctx.vmax = Math.max(ctx.vmax, b[3]);
    }
    const level = api.value(2), usd = api.value(3), ask = api.value(4), both = api.value(5);
    let top = api.coord([0, level + step])[1];
    let bot = api.coord([0, level])[1];
    if (both) {
      const mid = (top + bot) / 2;
      if (ask) bot = mid; else top = mid;
    }
    if (bot < cs.y || top > cs.y + cs.height) return null;   // уровень вне видимой шкалы цены
    const y0 = Math.max(top, cs.y), y1 = Math.min(bot, cs.y + cs.height);
    const h = y1 - y0;
    const x0 = cs.x + cs.width + 6;
    const maxW = BOOKW - 52;
    const w = Math.max(1, (usd / (ctx.vmax || 1)) * maxW);
    const kids = [{
      type: 'rect',
      shape: { x: x0, y: y0 + (h > 3 ? 0.5 : 0), width: w, height: Math.max(h - (h > 3 ? 1 : 0), 1) },
      style: { fill: ask ? ASK_BAR : BID_BAR },
    }];
    if (h >= 9) {
      kids.push({
        type: 'text',
        style: {
          x: x0 + w + 4, y: y0 + h / 2, text: fmtUsd(usd), fill: MUTED, align: 'left', verticalAlign: 'middle',
          font: `${Math.max(8, Math.min(11, Math.floor(h * 0.75)))}px sans-serif`,
        },
      });
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
  grid: { left: 8, right: 72 + BOOKW, top: BOOKW ? 24 : 10, bottom: 46 },
  tooltip: {
    trigger: 'item',
    confine: true,
    formatter: (p) => {
      const d = p.data;
      if (p.seriesName === 'book') {
        return `${d[4] ? 'Продают' : 'Покупают'} $${d[2]}–${d[2] + step}<br/><b>${fmtUsd(d[3])}</b> в заявках сейчас`;
      }
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
  xAxis: [{
    type: 'value',
    min: 0,
    max: n,
    interval: 1,
    axisLine: { lineStyle: { color: GRID } },
    axisTick: { show: false },
    splitLine: { show: true, lineStyle: { color: GRID } },
    axisLabel: { color: MUTED, formatter: (x) => (Number.isInteger(x) && x < n ? label(times[x]) : '') },
  }, { type: 'value', show: false, min: 0, max: 1 }],
  yAxis: {
    type: 'value',
    position: 'right',
    offset: BOOKW,
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
  graphic: BOOKW ? [{
    type: 'text', right: 72 + 8, top: 4, z: 5,
    style: {
      text: snapMs ? `стакан на ${label(snapMs)}:${pad(new Date(snapMs).getUTCSeconds())} UTC` : 'стакан сейчас',
      fill: MUTED, font: '11px sans-serif', align: 'right',
    },
  }] : [],
  series: [clusterSeries, candleSeries, bookSeries],
};
