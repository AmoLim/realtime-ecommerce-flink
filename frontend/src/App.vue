<template>
  <main class="dashboard">
    <header class="topbar">
      <div>
        <p class="eyebrow">PyFlink 实时计算大屏</p>
        <h1>电商订单监控与异常交易预警</h1>
      </div>
      <div class="status">
        <span :class="['status-dot', healthy ? 'ok' : 'bad']"></span>
        <span>{{ healthy ? '后端在线' : '连接中断' }}</span>
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
        <small>大额 / 支付 / 设备 / IP / 高频</small>
      </article>
      <article class="metric-card">
        <span>最新窗口结束</span>
        <strong class="time-value">{{ summary.latest_window_end || '--' }}</strong>
        <small>Flink 最新聚合窗口</small>
      </article>
    </section>

    <section class="chart-grid">
      <div class="panel wide">
        <div class="panel-heading">
          <h2>实时销售额趋势</h2>
        </div>
        <div ref="salesChartRef" class="chart"></div>
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
          </tbody>
        </table>
      </div>
    </section>
  </main>
</template>

<script setup>
import { nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import * as echarts from 'echarts'
import { apiGet } from './api'

const summary = ref({
  total_orders: 0,
  total_sales: 0,
  alert_count: 0,
  latest_window_orders: 0,
  latest_window_sales: 0,
  latest_window_end: null,
})
const salesTrend = ref([])
const topProducts = ref([])
const cityDistribution = ref([])
const latestOrders = ref([])
const alerts = ref([])
const healthy = ref(false)

const salesChartRef = ref(null)
const productChartRef = ref(null)
const cityChartRef = ref(null)

let salesChart
let productChart
let cityChart
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
}

function renderCharts() {
  salesChart?.setOption({
    color: ['#2563eb'],
    tooltip: { trigger: 'axis' },
    grid: { left: 48, right: 24, top: 32, bottom: 36 },
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
        name: '销售额',
        type: 'line',
        smooth: true,
        symbolSize: 7,
        areaStyle: { color: 'rgba(37, 99, 235, 0.12)' },
        data: salesTrend.value.map((item) => item.total_amount),
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
}

async function refresh() {
  try {
    const [health, nextSummary, trend, products, cities, orders, nextAlerts] = await Promise.all([
      apiGet('/api/health'),
      apiGet('/api/metrics/summary'),
      apiGet('/api/metrics/sales-trend?limit=24'),
      apiGet('/api/metrics/top-products?limit=8'),
      apiGet('/api/metrics/city-distribution'),
      apiGet('/api/orders/latest?limit=10'),
      apiGet('/api/alerts?limit=10'),
    ])
    healthy.value = health.status === 'ok'
    summary.value = nextSummary
    salesTrend.value = trend
    topProducts.value = products
    cityDistribution.value = cities
    latestOrders.value = orders
    alerts.value = nextAlerts
    renderCharts()
  } catch (error) {
    healthy.value = false
    console.error(error)
  }
}

function resizeCharts() {
  salesChart?.resize()
  productChart?.resize()
  cityChart?.resize()
}

onMounted(async () => {
  await nextTick()
  initCharts()
  await refresh()
  timer = window.setInterval(refresh, 3000)
  window.addEventListener('resize', resizeCharts)
})

onBeforeUnmount(() => {
  window.clearInterval(timer)
  window.removeEventListener('resize', resizeCharts)
  salesChart?.dispose()
  productChart?.dispose()
  cityChart?.dispose()
})
</script>
