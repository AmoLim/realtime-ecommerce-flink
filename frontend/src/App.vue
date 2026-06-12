<template>
  <main class="dashboard">
    <header class="topbar">
      <div>
        <p class="eyebrow">PyFlink 事件时间实时计算大屏</p>
        <h1>电商订单监控与异常交易预警</h1>
      </div>
      <div class="top-actions">
        <label class="toggle">
          <input v-model="autoRefresh" type="checkbox" />
          <span></span>
          自动刷新
        </label>
        <select v-model.number="refreshInterval" class="select">
          <option :value="3000">3 秒</option>
          <option :value="5000">5 秒</option>
          <option :value="10000">10 秒</option>
        </select>
        <button class="button" type="button" @click="refresh">刷新</button>
        <div class="status">
          <span :class="['status-dot', healthy ? 'ok' : 'bad']"></span>
          <span>{{ healthy ? '后端在线' : '连接中断' }}</span>
        </div>
      </div>
    </header>

    <section class="metrics-grid">
      <article class="metric-card">
        <span>累计订单</span>
        <strong>{{ formatInteger(summary.total_orders) }}</strong>
        <small>最新窗口 {{ formatInteger(summary.latest_window_orders) }} 单</small>
      </article>
      <article class="metric-card">
        <span>累计销售额</span>
        <strong>¥{{ formatMoney(summary.total_sales) }}</strong>
        <small>最新窗口 ¥{{ formatMoney(summary.latest_window_sales) }}</small>
      </article>
      <article class="metric-card">
        <span>异常告警</span>
        <strong>{{ formatInteger(summary.alert_count) }}</strong>
        <small>高风险 {{ formatInteger(summary.high_alert_count) }} 条</small>
      </article>
      <article class="metric-card">
        <span>事件时间进度</span>
        <strong class="time-value">{{ summary.latest_order_time || '--' }}</strong>
        <small>窗口结束 {{ summary.latest_window_end || '--' }}</small>
      </article>
    </section>

    <section class="control-band">
      <div class="segmented">
        <button :class="{ active: trendMetric === 'amount' }" type="button" @click="trendMetric = 'amount'">销售额</button>
        <button :class="{ active: trendMetric === 'orders' }" type="button" @click="trendMetric = 'orders'">订单数</button>
      </div>
      <div class="filters">
        <select v-model="riskFilter" class="select" @change="refreshAlerts">
          <option value="">全部风险</option>
          <option v-for="risk in alertOptions.risk_levels" :key="risk" :value="risk">{{ risk }}</option>
        </select>
        <select v-model="alertTypeFilter" class="select wide-select" @change="refreshAlerts">
          <option value="">全部类型</option>
          <option v-for="type in alertOptions.alert_types" :key="type" :value="type">{{ type }}</option>
        </select>
        <button class="button ghost" type="button" @click="clearFilters">清除筛选</button>
      </div>
    </section>

    <section class="chart-grid">
      <div class="panel wide">
        <div class="panel-heading">
          <h2>实时趋势</h2>
        </div>
        <div ref="salesChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-heading">
          <h2>告警类型分布</h2>
        </div>
        <div ref="alertChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-heading">
          <h2>风险趋势</h2>
        </div>
        <div ref="riskChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-heading">
          <h2>热门商品</h2>
        </div>
        <div ref="productChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-heading">
          <h2>城市销售分布</h2>
        </div>
        <div ref="cityChartRef" class="chart"></div>
      </div>
    </section>

    <section class="table-grid">
      <div class="panel">
        <div class="panel-heading">
          <h2>最新订单</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>订单</th>
              <th>商品</th>
              <th>城市</th>
              <th>金额</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="order in latestOrders" :key="order.order_id">
              <td>{{ order.order_id }}</td>
              <td>{{ order.product_name }}</td>
              <td>{{ order.city }}</td>
              <td>¥{{ formatMoney(order.amount) }}</td>
              <td>
                <span :class="['tag', order.payment_status === 'SUCCESS' ? 'success' : 'warning']">
                  {{ order.payment_status }}
                </span>
              </td>
            </tr>
            <tr v-if="!latestOrders.length">
              <td colspan="5" class="empty-state">暂无订单</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="panel">
        <div class="panel-heading">
          <h2>异常告警</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>风险</th>
              <th>用户</th>
              <th>类型</th>
              <th>原因</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="alert in alerts" :key="alert.alert_id">
              <td>
                <span :class="['tag', alert.risk_level === 'HIGH' ? 'danger' : 'warning']">
                  {{ alert.risk_level }}
                </span>
              </td>
              <td>{{ alert.user_id }}</td>
              <td>{{ alert.alert_type }}</td>
              <td>{{ alert.reason }}</td>
            </tr>
            <tr v-if="!alerts.length">
              <td colspan="4" class="empty-state">暂无告警</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>
</template>

<script setup>
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { apiGet } from './api'

const summary = ref({
  total_orders: 0,
  total_sales: 0,
  alert_count: 0,
  high_alert_count: 0,
  latest_window_orders: 0,
  latest_window_sales: 0,
  latest_window_end: null,
  latest_order_time: null,
})
const salesTrend = ref([])
const topProducts = ref([])
const cityDistribution = ref([])
const alertDistribution = ref([])
const riskTrend = ref([])
const latestOrders = ref([])
const alerts = ref([])
const alertOptions = ref({ risk_levels: [], alert_types: [] })
const healthy = ref(false)
const autoRefresh = ref(true)
const refreshInterval = ref(3000)
const trendMetric = ref('amount')
const riskFilter = ref('')
const alertTypeFilter = ref('')

const salesChartRef = ref(null)
const productChartRef = ref(null)
const cityChartRef = ref(null)
const alertChartRef = ref(null)
const riskChartRef = ref(null)

let salesChart
let productChart
let cityChart
let alertChart
let riskChart
let timer

function formatMoney(value) {
  return Number(value || 0).toLocaleString('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
}

function formatInteger(value) {
  return Number(value || 0).toLocaleString('zh-CN')
}

function initCharts() {
  salesChart = echarts.init(salesChartRef.value)
  productChart = echarts.init(productChartRef.value)
  cityChart = echarts.init(cityChartRef.value)
  alertChart = echarts.init(alertChartRef.value)
  riskChart = echarts.init(riskChartRef.value)
}

function renderCharts() {
  const trendName = trendMetric.value === 'amount' ? '销售额' : '订单数'
  const trendData = salesTrend.value.map((item) =>
    trendMetric.value === 'amount' ? item.total_amount : item.order_count,
  )

  salesChart?.setOption({
    color: ['#2563eb'],
    tooltip: { trigger: 'axis' },
    grid: { left: 52, right: 24, top: 32, bottom: 36 },
    xAxis: {
      type: 'category',
      data: salesTrend.value.map((item) => item.time?.slice(11) || item.time),
      axisLabel: { color: '#5f6b7a' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#5f6b7a' },
      splitLine: { lineStyle: { color: '#e7edf5' } },
    },
    series: [
      {
        name: trendName,
        type: 'line',
        smooth: true,
        symbolSize: 7,
        areaStyle: { color: 'rgba(37, 99, 235, 0.12)' },
        data: trendData,
      },
    ],
  })

  productChart?.setOption({
    color: ['#0f766e'],
    tooltip: { trigger: 'axis' },
    grid: { left: 56, right: 18, top: 28, bottom: 72 },
    xAxis: {
      type: 'category',
      data: topProducts.value.map((item) => item.product_name),
      axisLabel: { color: '#5f6b7a', rotate: 35 },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#5f6b7a' },
      splitLine: { lineStyle: { color: '#e7edf5' } },
    },
    series: [
      {
        name: '销售额',
        type: 'bar',
        barWidth: 18,
        data: topProducts.value.map((item) => item.total_amount),
      },
    ],
  })

  cityChart?.setOption({
    color: ['#2563eb', '#0f766e', '#f59e0b', '#dc2626', '#7c3aed', '#0891b2', '#65a30d', '#db2777'],
    tooltip: { trigger: 'item' },
    series: [
      {
        name: '城市销售额',
        type: 'pie',
        radius: ['42%', '70%'],
        center: ['50%', '52%'],
        label: { color: '#334155', formatter: '{b}' },
        data: cityDistribution.value.map((item) => ({
          name: item.city,
          value: item.total_amount,
        })),
      },
    ],
  })

  alertChart?.setOption({
    color: ['#dc2626'],
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 16, top: 28, bottom: 86 },
    xAxis: {
      type: 'category',
      data: alertDistribution.value.map((item) => item.alert_type),
      axisLabel: { color: '#5f6b7a', rotate: 35 },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#5f6b7a' },
      splitLine: { lineStyle: { color: '#e7edf5' } },
    },
    series: [
      {
        name: '告警数',
        type: 'bar',
        barWidth: 16,
        data: alertDistribution.value.map((item) => item.alert_count),
      },
    ],
  })

  riskChart?.setOption({
    color: ['#dc2626', '#f59e0b'],
    tooltip: { trigger: 'axis' },
    legend: { top: 0, textStyle: { color: '#64748b' } },
    grid: { left: 42, right: 16, top: 38, bottom: 42 },
    xAxis: {
      type: 'category',
      data: riskTrend.value.map((item) => item.time?.slice(11) || item.time),
      axisLabel: { color: '#5f6b7a' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#5f6b7a' },
      splitLine: { lineStyle: { color: '#e7edf5' } },
    },
    series: [
      { name: 'HIGH', type: 'bar', stack: 'risk', data: riskTrend.value.map((item) => item.high_count) },
      { name: 'MEDIUM', type: 'bar', stack: 'risk', data: riskTrend.value.map((item) => item.medium_count) },
    ],
  })
}

async function refreshAlerts() {
  const params = new URLSearchParams({ limit: '12' })
  if (riskFilter.value) {
    params.set('risk_level', riskFilter.value)
  }
  if (alertTypeFilter.value) {
    params.set('alert_type', alertTypeFilter.value)
  }
  alerts.value = await apiGet(`/api/alerts?${params.toString()}`)
}

async function refresh() {
  try {
    const [health, nextSummary, trend, products, cities, alertStats, riskStats, orders, options] = await Promise.all([
      apiGet('/api/health'),
      apiGet('/api/metrics/summary'),
      apiGet('/api/metrics/sales-trend?limit=24'),
      apiGet('/api/metrics/top-products?limit=8'),
      apiGet('/api/metrics/city-distribution'),
      apiGet('/api/metrics/alert-distribution'),
      apiGet('/api/metrics/risk-trend?limit=24'),
      apiGet('/api/orders/latest?limit=10'),
      apiGet('/api/alerts/options'),
    ])
    healthy.value = health.status === 'ok'
    summary.value = nextSummary
    salesTrend.value = trend
    topProducts.value = products
    cityDistribution.value = cities
    alertDistribution.value = alertStats
    riskTrend.value = riskStats
    latestOrders.value = orders
    alertOptions.value = options
    await refreshAlerts()
    renderCharts()
  } catch (error) {
    healthy.value = false
    console.error(error)
  }
}

function clearFilters() {
  riskFilter.value = ''
  alertTypeFilter.value = ''
  refreshAlerts()
}

function resizeCharts() {
  salesChart?.resize()
  productChart?.resize()
  cityChart?.resize()
  alertChart?.resize()
  riskChart?.resize()
}

function scheduleRefresh() {
  window.clearInterval(timer)
  if (autoRefresh.value) {
    timer = window.setInterval(refresh, refreshInterval.value)
  }
}

watch([autoRefresh, refreshInterval], scheduleRefresh)
watch(trendMetric, renderCharts)

onMounted(async () => {
  await nextTick()
  initCharts()
  await refresh()
  scheduleRefresh()
  window.addEventListener('resize', resizeCharts)
})

onBeforeUnmount(() => {
  window.clearInterval(timer)
  window.removeEventListener('resize', resizeCharts)
  salesChart?.dispose()
  productChart?.dispose()
  cityChart?.dispose()
  alertChart?.dispose()
  riskChart?.dispose()
})
</script>
