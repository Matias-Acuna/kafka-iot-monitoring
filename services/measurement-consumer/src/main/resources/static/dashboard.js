/*
 * Expected GET /metrics/dashboard contract (proposed; backend not implemented):
 * {
 *   runId: string|null, observedRule: string|null,
 *   totalResults: number|null, uniqueResults: number|null,
 *   duplicateResults: number|null, totalAlerts: number|null,
 *   alertsByRule: { HIGH_TEMPERATURE: number|null,
 *                   HIGH_FREQUENCY: number|null, RAPID_CHANGE: number|null },
 *   resultThroughputPerSecond: number|null,
 *   latencyP50Ms: number|null, latencyP95Ms: number|null, latencyP99Ms: number|null,
 *   throughputSeries: [{ timestamp: number, value: number|null }],
 *   latencySeries: [{ timestamp: number, p50Ms: number|null,
 *                     p95Ms: number|null, p99Ms: number|null }],
 *   lastUpdatedAt: number|null
 * }
 * All timestamps are Unix epoch milliseconds. Counts are nonnegative safe
 * integers; rates are nonnegative finite numbers; percentiles are finite
 * numbers supplied by the backend, never calculated or capped here. Null or
 * absent values are unavailable, not zero. Series are backend snapshots, not
 * deltas. Keep at most the latest 120 supplied points; never synthesize history.
 * Connection status describes HTTP metrics access, never Kafka health.
 */
(() => {
  'use strict';

  const ENDPOINT = '/metrics/dashboard';
  const REFRESH_MS = 3000;
  const REQUEST_TIMEOUT_MS = 8000;
  const MAX_POINTS = 120;
  const integerFormat = new Intl.NumberFormat('en-US');
  const decimalFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
  const clockFormat = new Intl.DateTimeFormat('en-GB', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, timeZone: 'UTC'
  });
  const rules = { HIGH_TEMPERATURE: 'alerts-temperature', HIGH_FREQUENCY: 'alerts-frequency', RAPID_CHANGE: 'alerts-change' };
  const counts = { totalResults: 'total-results', uniqueResults: 'unique-results', duplicateResults: 'duplicate-results', totalAlerts: 'total-alerts' };
  const latencies = { latencyP50Ms: 'latency-p50', latencyP95Ms: 'latency-p95', latencyP99Ms: 'latency-p99' };
  let throughputChart = null;
  let latencyChart = null;
  let inFlight = false;
  let refreshTimer = null;
  let currentRequest = null;
  let stopped = false;
  let lastSuccess = null;
  let lastBackendTimestamp = null;

  const element = (id) => document.getElementById(id);
  const finite = (value) => typeof value === 'number' && Number.isFinite(value);
  const count = (value) => Number.isSafeInteger(value) && value >= 0;
  const rate = (value) => finite(value) && value >= 0;
  const text = (value) => typeof value === 'string' && value.trim() ? value : 'N/A';
  const timestamp = (value) => Number.isSafeInteger(value) && value >= 0 && value <= 8640000000000000;
  const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

  function showNumber(id, value, valid, format = decimalFormat) {
    element(id).textContent = valid(value) ? format.format(value) : 'N/A';
  }

  function showTime(id, value) {
    const target = element(id);
    target.textContent = timestamp(value)
      ? `${id === 'last-data-update' ? new Date(value).toISOString().slice(0, 10) + ' ' : ''}${clockFormat.format(new Date(value))} UTC`
      : 'N/A';
    if (timestamp(value)) {
      target.setAttribute('datetime', new Date(value).toISOString());
      target.title = new Date(value).toISOString();
    } else {
      target.removeAttribute('datetime');
      target.removeAttribute('title');
    }
  }

  function setConnection(state, message) {
    const label = { connected: 'Connected', disconnected: 'Disconnected', waiting: 'Waiting for data' }[state];
    document.body.dataset.connection = state;
    element('connection-text').textContent = label;
    element('detail-connection').textContent = label;
    element('feed-title').textContent = state === 'disconnected' ? 'Metrics connection interrupted' : 'Your stream, at a glance';
    element('feed-message').textContent = message;
  }

  function chartOptions(yTitle, unit) {
    return {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'nearest', intersect: false, axis: 'x' },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#101827', titleColor: '#ffffff', bodyColor: '#dce8ff',
          borderColor: '#354869', borderWidth: 1, padding: 12,
          callbacks: {
            title: (items) => items.length ? new Date(items[0].parsed.x).toISOString() : '',
            label: (item) => `${item.dataset.label}: ${decimalFormat.format(item.parsed.y)} ${unit}`
          }
        }
      },
      scales: {
        // Linear epoch axis avoids a date adapter or an additional dependency.
        x: {
          type: 'linear', title: { display: true, text: 'Time (UTC)', color: '#a9b8d0', font: { size: 10 } },
          grid: { display: false }, border: { display: false },
          ticks: { color: '#a9b8d0', maxTicksLimit: 5, maxRotation: 0, font: { size: 9 },
            callback: (value) => timestamp(value) ? clockFormat.format(new Date(value)) : '' }
        },
        y: {
          title: { display: true, text: yTitle, color: '#a9b8d0', font: { size: 10 } },
          border: { display: false }, grid: { color: 'rgba(169,184,208,0.09)' },
          ticks: { color: '#a9b8d0', maxTicksLimit: 5, padding: 8, font: { size: 9 } }
        }
      }
    };
  }

  function dataset(label, color, fill = false) {
    return { label, data: [], borderColor: color, backgroundColor: fill ? 'rgba(70,135,255,0.12)' : color,
      fill, borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, tension: 0.25, spanGaps: false };
  }

  function initializeCharts() {
    if (typeof window.Chart !== 'function') {
      for (const id of ['throughput-empty', 'latency-empty']) {
        element(id).querySelector('strong').textContent = 'Chart library unavailable';
        element(id).querySelector('span:last-child').textContent = 'Check the packaged local Chart.js file.';
      }
      return;
    }
    try {
      throughputChart = new window.Chart(element('throughput-chart'), {
        type: 'line', data: { datasets: [dataset('Results throughput', '#4687ff', true)] },
        options: chartOptions('Results per second', 'results/s')
      });
      latencyChart = new window.Chart(element('latency-chart'), {
        type: 'line', data: { datasets: [dataset('P50', '#4687ff'), dataset('P95', '#55d7e8'), dataset('P99', '#b49aff')] },
        options: chartOptions('Latency (ms)', 'ms')
      });
    } catch (error) {
      // Keep the metrics view usable if canvas initialization is unavailable.
      if (throughputChart) throughputChart.destroy();
      if (latencyChart) latencyChart.destroy();
      throughputChart = null;
      latencyChart = null;
      for (const id of ['throughput-empty', 'latency-empty']) {
        element(id).querySelector('strong').textContent = 'Chart rendering unavailable';
      }
    }
  }

  function samples(value) {
    if (!Array.isArray(value)) return [];
    return value.filter((sample) => object(sample) && timestamp(sample.timestamp))
      .sort((a, b) => a.timestamp - b.timestamp).slice(-MAX_POINTS);
  }

  function updateCharts(data) {
    if (throughputChart) {
      const points = samples(data.throughputSeries).map((sample) => ({ x: sample.timestamp, y: rate(sample.value) ? sample.value : null }));
      throughputChart.data.datasets[0].data = points;
      throughputChart.update('none');
      element('throughput-empty').hidden = points.some((point) => point.y !== null);
    }
    if (latencyChart) {
      const points = samples(data.latencySeries);
      ['p50Ms', 'p95Ms', 'p99Ms'].forEach((name, index) => {
        latencyChart.data.datasets[index].data = points.map((sample) => ({ x: sample.timestamp, y: finite(sample[name]) ? sample[name] : null }));
      });
      latencyChart.update('none');
      element('latency-empty').hidden = points.some((point) => ['p50Ms', 'p95Ms', 'p99Ms'].some((name) => finite(point[name])));
    }
  }

  function render(data) {
    Object.entries(counts).forEach(([field, id]) => showNumber(id, data[field], count, integerFormat));
    Object.entries(latencies).forEach(([field, id]) => showNumber(id, data[field], finite));
    showNumber('result-throughput', data.resultThroughputPerSecond, rate);
    const alerts = object(data.alertsByRule) ? data.alertsByRule : {};
    Object.entries(rules).forEach(([rule, id]) => showNumber(id, alerts[rule], count, integerFormat));
    element('run-id').textContent = text(data.runId);
    element('header-run').textContent = text(data.runId);
    element('observed-rule').textContent = text(data.observedRule);
    showTime('last-data-update', data.lastUpdatedAt);
    updateCharts(data);
    const hasMetrics = Object.keys(counts).some((key) => count(data[key]))
      || Object.keys(latencies).some((key) => finite(data[key])) || rate(data.resultThroughputPerSecond)
      || Object.keys(rules).some((rule) => count(alerts[rule]))
      || samples(data.throughputSeries).some((point) => rate(point.value))
      || samples(data.latencySeries).some((point) => ['p50Ms', 'p95Ms', 'p99Ms'].some((key) => finite(point[key])));
    const backendTimestamp = timestamp(data.lastUpdatedAt) ? data.lastUpdatedAt : null;
    const freshness = backendTimestamp === null ? ' Backend data freshness is unavailable.'
      : backendTimestamp === lastBackendTimestamp ? ' The backend data timestamp has not advanced.' : '';
    setConnection(hasMetrics ? 'connected' : 'waiting', (hasMetrics
      ? 'Metrics endpoint connected. Displaying backend-supplied operational observations.'
      : 'Metrics endpoint connected; waiting for available observations. Missing values are not zero.') + freshness);
    lastBackendTimestamp = backendTimestamp;
  }

  async function refresh() {
    if (inFlight || stopped) return;
    inFlight = true;
    window.clearTimeout(refreshTimer);
    element('refresh-button').disabled = true;
    currentRequest = new window.AbortController();
    const timeout = window.setTimeout(() => currentRequest.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await window.fetch(ENDPOINT, { headers: { Accept: 'application/json' }, cache: 'no-store', signal: currentRequest.signal });
      if (!response.ok) throw new Error(`Metrics endpoint returned HTTP ${response.status}.`);
      const data = await response.json();
      if (!object(data)) throw new Error('Metrics endpoint returned an invalid JSON object.');
      if (stopped) return;
      render(data);
      lastSuccess = Date.now();
      showTime('last-success', lastSuccess);
    } catch (error) {
      if (!stopped) {
        const reason = error.name === 'AbortError' ? 'Metrics request timed out.' : error.message;
        const retained = lastSuccess === null ? 'No metrics have been received.' : 'Previously received values are retained and may be stale.';
        setConnection('disconnected', `${reason} ${retained}`);
      }
    } finally {
      window.clearTimeout(timeout);
      currentRequest = null;
      inFlight = false;
      element('refresh-button').disabled = false;
      if (!stopped) refreshTimer = window.setTimeout(refresh, REFRESH_MS);
    }
  }

  element('refresh-button').addEventListener('click', refresh);
  window.addEventListener('pagehide', () => {
    stopped = true;
    window.clearTimeout(refreshTimer);
    if (currentRequest) currentRequest.abort();
    if (throughputChart) throughputChart.destroy();
    if (latencyChart) latencyChart.destroy();
  }, { once: true });
  initializeCharts();
  refresh();
})();
