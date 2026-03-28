/* ═══════════════════════════════════════════════════════
   phlower — bookmarks, Chart.js, SSE-driven refresh
   ═══════════════════════════════════════════════════════ */

// -- bookmarks (localStorage) ------------------------------------------

function getBookmarks() {
  try { return JSON.parse(localStorage.getItem('phlower_bookmarks') || '[]'); }
  catch { return []; }
}

function saveBookmarks(bms) {
  localStorage.setItem('phlower_bookmarks', JSON.stringify(bms));
}

var BM_SVG = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>';
var BM_SVG_FILLED = '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>';

function toggleBookmark(taskName) {
  var bms = getBookmarks();
  var idx = bms.indexOf(taskName);
  if (idx === -1) { bms.push(taskName); } else { bms.splice(idx, 1); }
  saveBookmarks(bms);
  applyBookmarks();
  // Also update detail-page button if present
  var detailBtn = document.getElementById('bm-detail-btn');
  if (detailBtn) {
    var active = bms.indexOf(taskName) !== -1;
    detailBtn.innerHTML = active ? BM_SVG_FILLED : BM_SVG;
    detailBtn.classList.toggle('bm-active', active);
  }
}

function applyBookmarks() {
  var bms = getBookmarks();
  var table = document.getElementById('task-table');
  if (!table) return;

  var tbody = table.querySelector('tbody');
  if (!tbody) return;

  var rows = Array.from(tbody.querySelectorAll('tr[data-task]'));
  var bookmarked = [];
  var rest = [];

  rows.forEach(function(row) {
    var task = row.getAttribute('data-task');
    var btn = row.querySelector('.bm-btn');
    var isBm = bms.indexOf(task) !== -1;
    if (btn) {
      btn.innerHTML = isBm ? BM_SVG_FILLED : BM_SVG;
      btn.classList.toggle('bm-active', isBm);
    }
    row.classList.toggle('bm-row', isBm);
    if (isBm) { bookmarked.push(row); } else { rest.push(row); }
  });

  // Re-order: bookmarked first
  bookmarked.concat(rest).forEach(function(row) { tbody.appendChild(row); });
}

let latencyChart = null;
let throughputChart = null;

const C = {
  p50:     '#1D4AFF',
  p95:     '#7B61FF',
  p99:     '#F54E00',
  success: '#28A745',
  failure: '#E5210C',
  retry:   '#D4930D',
  grid:    '#E2E1DC',
  tick:    '#9B9DA3',
};

const BASE_OPTS = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 200 },
  interaction: { mode: 'index', intersect: false },
  plugins: {
    legend: {
      position: 'bottom',
      labels: {
        boxWidth: 8,
        boxHeight: 8,
        padding: 12,
        font: { family: "'Instrument Sans', sans-serif", size: 10, weight: '600' },
        color: '#5C5F66',
        usePointStyle: true,
        pointStyle: 'rect',
      },
    },
    tooltip: {
      backgroundColor: '#1D1F27',
      titleFont: { family: "'JetBrains Mono', monospace", size: 10, weight: '600' },
      bodyFont: { family: "'JetBrains Mono', monospace", size: 10 },
      padding: { top: 6, bottom: 6, left: 8, right: 8 },
      cornerRadius: 3,
      displayColors: true,
      boxWidth: 6,
      boxHeight: 6,
      boxPadding: 4,
    },
  },
  scales: {
    x: {
      type: 'time',
      time: { unit: 'minute', displayFormats: { minute: 'HH:mm' } },
      grid: { display: false },
      border: { color: C.grid },
      ticks: {
        font: { family: "'JetBrains Mono', monospace", size: 9.5 },
        color: C.tick,
        maxRotation: 0,
      },
    },
  },
};

function ts(epoch) { return new Date(epoch * 1000); }

function initCharts(data) {
  const lCtx = document.getElementById('latency-chart');
  const tCtx = document.getElementById('throughput-chart');
  if (!lCtx || !tCtx) return;

  const labels = data.map(d => ts(d.t));

  latencyChart = new Chart(lCtx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'p50',
          data: data.map(d => d.p50),
          borderColor: C.p50,
          backgroundColor: C.p50 + '10',
          borderWidth: 1.5,
          pointRadius: 0,
          pointHoverRadius: 3,
          fill: true,
          tension: 0.3,
        },
        {
          label: 'p95',
          data: data.map(d => d.p95),
          borderColor: C.p95,
          borderWidth: 1.5,
          borderDash: [4, 2],
          pointRadius: 0,
          pointHoverRadius: 3,
          fill: false,
          tension: 0.3,
        },
        {
          label: 'p99',
          data: data.map(d => d.p99),
          borderColor: C.p99,
          borderWidth: 1.5,
          borderDash: [2, 2],
          pointRadius: 0,
          pointHoverRadius: 3,
          fill: false,
          tension: 0.3,
        },
      ],
    },
    options: {
      ...BASE_OPTS,
      scales: {
        ...BASE_OPTS.scales,
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'ms',
            font: { family: "'JetBrains Mono', monospace", size: 9.5, weight: '600' },
            color: C.tick,
          },
          grid: { color: C.grid + '80', lineWidth: 0.5 },
          border: { display: false },
          ticks: {
            font: { family: "'JetBrains Mono', monospace", size: 9.5 },
            color: C.tick,
            padding: 4,
          },
        },
      },
    },
  });

  throughputChart = new Chart(tCtx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Success',
          data: data.map(d => d.success),
          backgroundColor: C.success + 'B0',
          borderRadius: 1,
          borderSkipped: false,
        },
        {
          label: 'Failure',
          data: data.map(d => d.failure),
          backgroundColor: C.failure + 'CC',
          borderRadius: 1,
          borderSkipped: false,
        },
        {
          label: 'Retry',
          data: data.map(d => d.retry),
          backgroundColor: C.retry + 'B0',
          borderRadius: 1,
          borderSkipped: false,
        },
      ],
    },
    options: {
      ...BASE_OPTS,
      scales: {
        ...BASE_OPTS.scales,
        x: {
          ...BASE_OPTS.scales.x,
          stacked: true,
        },
        y: {
          stacked: true,
          beginAtZero: true,
          title: {
            display: true,
            text: 'count',
            font: { family: "'JetBrains Mono', monospace", size: 9.5, weight: '600' },
            color: C.tick,
          },
          grid: { color: C.grid + '80', lineWidth: 0.5 },
          border: { display: false },
          ticks: {
            font: { family: "'JetBrains Mono', monospace", size: 9.5 },
            color: C.tick,
            padding: 4,
            stepSize: 1,
          },
        },
      },
    },
  });
}

function refreshCharts(taskName) {
  if (!latencyChart || !throughputChart) return;

  fetch('/api/tasks/' + encodeURIComponent(taskName) + '/latency')
    .then(r => r.json())
    .then(data => {
      const labels = data.map(d => ts(d.t));

      latencyChart.data.labels = labels;
      latencyChart.data.datasets[0].data = data.map(d => d.p50);
      latencyChart.data.datasets[1].data = data.map(d => d.p95);
      latencyChart.data.datasets[2].data = data.map(d => d.p99);
      latencyChart.update('none');

      throughputChart.data.labels = labels;
      throughputChart.data.datasets[0].data = data.map(d => d.success);
      throughputChart.data.datasets[1].data = data.map(d => d.failure);
      throughputChart.data.datasets[2].data = data.map(d => d.retry);
      throughputChart.update('none');
    })
    .catch(() => {});
}
