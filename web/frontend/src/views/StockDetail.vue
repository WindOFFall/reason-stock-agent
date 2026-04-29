<template>
  <div>
    <!-- 返回列 + 標題 -->
    <div class="top-bar">
      <span class="back-btn" @click="$router.back()">← 返回</span>
      <span class="page-title">
        <span class="stock-id-label">{{ stockId }}</span>
        <span class="stock-name-label">{{ stockName }}</span>
      </span>
    </div>

    <!-- K 線圖 -->
    <el-card style="margin-bottom:16px">
      <template #header>
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span>股價走勢</span>
          <div style="display:flex;gap:6px">
            <span v-for="tag in indicatorTags" :key="tag.label" class="ind-tag" :style="{color: tag.color}">
              {{ tag.label }}: <b>{{ tag.value }}</b>
            </span>
          </div>
        </div>
      </template>
      <div ref="priceChartRef" style="height:320px" v-loading="loadingPrice" />
    </el-card>

    <!-- 法人 + EPS -->
    <el-row :gutter="16">
      <el-col :span="14">
        <el-card header="三大法人（近 30 天）" style="margin-bottom:16px">
          <div ref="instChartRef" style="height:200px" v-loading="loadingInst" />
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card header="近 8 季 EPS" style="margin-bottom:16px">
          <div ref="epsChartRef" style="height:200px" v-loading="loadingEps" />
        </el-card>
      </el-col>
    </el-row>

    <!-- 最新 10 日行情表 -->
    <el-card header="最新 10 日行情">
      <el-table :data="recentPrices" size="small" stripe>
        <el-table-column prop="date"   label="日期"  width="120" />
        <el-table-column prop="open"   label="開盤"  width="90">
          <template #default="{ row }"><span class="num">{{ row.open }}</span></template>
        </el-table-column>
        <el-table-column prop="high"   label="最高"  width="90">
          <template #default="{ row }"><span class="up">{{ row.high }}</span></template>
        </el-table-column>
        <el-table-column prop="low"    label="最低"  width="90">
          <template #default="{ row }"><span class="down">{{ row.low }}</span></template>
        </el-table-column>
        <el-table-column prop="close"  label="收盤"  width="90">
          <template #default="{ row }">
            <span :class="row.close >= row.open ? 'up' : 'down'">{{ row.close }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="volume" label="成交量">
          <template #default="{ row }">
            <span class="num">{{ row.volume?.toLocaleString() }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import * as echarts from 'echarts'
import { getStockPrice, getInstitutional, getEps, getIndicators } from '../api/index.js'

const route   = useRoute()
const stockId = route.params.id

const priceData = ref([])
const instData  = ref([])
const epsData   = ref([])
const indData   = ref([])
const stockName = ref('')

const loadingPrice = ref(true)
const loadingInst  = ref(true)
const loadingEps   = ref(true)

const priceChartRef = ref(null)
const instChartRef  = ref(null)
const epsChartRef   = ref(null)
let priceChart = null
let instChart  = null
let epsChart   = null

const recentPrices = computed(() => priceData.value.slice(-10).reverse())

const indicatorTags = computed(() => {
  const last = indData.value.at(-1)
  if (!last) return []
  return [
    { label: 'RSI', value: last.rsi?.toFixed(1) ?? '—', color: last.rsi > 70 ? '#ff4d6d' : last.rsi < 30 ? '#00ff88' : '#c8d6e5' },
    { label: 'MA20', value: last.ma20?.toFixed(1) ?? '—', color: '#00d4ff' },
    { label: '量比', value: last.vol_ratio?.toFixed(2) ?? '—', color: last.vol_ratio > 2 ? '#f6c90e' : '#c8d6e5' },
  ]
})

// ── 股價 K 線 ────────────────────────────────────────────────
function drawPriceChart() {
  if (!priceChartRef.value || !priceData.value.length) return
  if (!priceChart) priceChart = echarts.init(priceChartRef.value)

  const prices = priceData.value
  const ind    = indData.value
  const dates  = prices.map(d => d.date)

  const findInd = (date) => ind.find(i => i.date === date) ?? {}

  const priceChart_opt = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'cross' },
      backgroundColor: '#0d1220', borderColor: 'rgba(0,212,255,0.2)',
      textStyle: { color: '#c8d6e5', fontSize: 11 },
    },
    legend: { data: ['K線', 'MA5', 'MA20', 'MA60'], textStyle: { color: '#5a7080', fontSize: 11 }, top: 0 },
    grid: { left: 60, right: 20, top: 30, bottom: 20 },
    xAxis: {
      type: 'category', data: dates,
      axisLabel: { color: '#5a7080', fontSize: 10 },
      axisLine: { lineStyle: { color: 'rgba(0,212,255,0.1)' } },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value', scale: true,
      axisLabel: { color: '#5a7080', fontSize: 10 },
      splitLine: { lineStyle: { color: 'rgba(0,212,255,0.05)' } },
    },
    series: [
      {
        name: 'K線', type: 'candlestick',
        data: prices.map(d => [d.open, d.close, d.low, d.high]),
        itemStyle: { color: '#ff4d6d', color0: '#00ff88', borderColor: '#ff4d6d', borderColor0: '#00ff88' },
      },
      { name: 'MA5',  type: 'line', data: dates.map(d => findInd(d).ma5?.toFixed(2)), smooth: true, symbol: 'none', lineStyle: { width: 1, color: '#f6c90e' }, z: 3 },
      { name: 'MA20', type: 'line', data: dates.map(d => findInd(d).ma20?.toFixed(2)), smooth: true, symbol: 'none', lineStyle: { width: 1.5, color: '#00d4ff' }, z: 3 },
      { name: 'MA60', type: 'line', data: dates.map(d => findInd(d).ma60?.toFixed(2)), smooth: true, symbol: 'none', lineStyle: { width: 1.5, color: '#a78bfa' }, z: 3 },
    ],
  }
  priceChart.setOption(priceChart_opt)
}

// ── 法人柱狀圖 ───────────────────────────────────────────────
function drawInstChart() {
  if (!instChartRef.value || !instData.value.length) return
  if (!instChart) instChart = echarts.init(instChartRef.value)

  const data  = instData.value.slice(-30)
  const dates = data.map(d => d.date.slice(5))

  instChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: '#0d1220', borderColor: 'rgba(0,212,255,0.2)', textStyle: { color: '#c8d6e5', fontSize: 11 } },
    legend: { data: ['外資', '投信', '自營商'], textStyle: { color: '#5a7080', fontSize: 10 }, top: 0 },
    grid: { left: 60, right: 10, top: 28, bottom: 20 },
    xAxis: { type: 'category', data: dates, axisLabel: { color: '#5a7080', fontSize: 9, rotate: 30 }, axisLine: { lineStyle: { color: 'rgba(0,212,255,0.1)' } }, splitLine: { show: false } },
    yAxis: { type: 'value', axisLabel: { color: '#5a7080', fontSize: 10 }, splitLine: { lineStyle: { color: 'rgba(0,212,255,0.05)' } } },
    series: [
      { name: '外資',   type: 'bar', data: data.map(d => d.foreign_net), itemStyle: { color: '#00d4ff' }, barMaxWidth: 8 },
      { name: '投信',   type: 'bar', data: data.map(d => d.invest_net),  itemStyle: { color: '#00ff88' }, barMaxWidth: 8 },
      { name: '自營商', type: 'bar', data: data.map(d => d.dealer_net),  itemStyle: { color: '#f6c90e' }, barMaxWidth: 8 },
    ],
  })
}

// ── EPS 柱狀圖 ───────────────────────────────────────────────
function drawEpsChart() {
  if (!epsChartRef.value || !epsData.value.length) return
  if (!epsChart) epsChart = echarts.init(epsChartRef.value)

  const data  = epsData.value
  const dates = data.map(d => d.date.slice(0, 7))
  const vals  = data.map(d => d.value)

  epsChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: '#0d1220', borderColor: 'rgba(0,212,255,0.2)', textStyle: { color: '#c8d6e5', fontSize: 11 } },
    grid: { left: 50, right: 10, top: 16, bottom: 36 },
    xAxis: { type: 'category', data: dates, axisLabel: { color: '#5a7080', fontSize: 9, rotate: 30 }, axisLine: { lineStyle: { color: 'rgba(0,212,255,0.1)' } }, splitLine: { show: false } },
    yAxis: { type: 'value', axisLabel: { color: '#5a7080', fontSize: 10 }, splitLine: { lineStyle: { color: 'rgba(0,212,255,0.05)' } } },
    series: [{
      type: 'bar', data: vals,
      itemStyle: { color: (p) => p.value >= 0 ? 'rgba(255,77,109,0.8)' : 'rgba(0,255,136,0.8)' },
      barMaxWidth: 28,
      label: { show: true, position: 'top', color: '#c8d6e5', fontSize: 10, formatter: v => v.value?.toFixed(2) },
    }],
  })
}

onMounted(async () => {
  const [p, i, e, ind] = await Promise.allSettled([
    getStockPrice(stockId, 90),
    getInstitutional(stockId, 30),
    getEps(stockId),
    getIndicators(stockId, 120),
  ])

  if (p.status === 'fulfilled') {
    priceData.value = p.value.data
    stockName.value = p.value.data[0]?.name ?? ''
  }
  loadingPrice.value = false

  if (i.status === 'fulfilled') instData.value = i.value.data
  loadingInst.value = false

  if (e.status === 'fulfilled') epsData.value = e.value.data
  loadingEps.value = false

  if (ind.status === 'fulfilled') indData.value = ind.value.data

  await nextTick()
  drawPriceChart()
  drawInstChart()
  drawEpsChart()
})
</script>

<style scoped>
.top-bar {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 20px;
}
.back-btn {
  font-size: 13px;
  color: rgba(0,212,255,0.6);
  cursor: pointer;
  letter-spacing: 0.5px;
  transition: color 0.2s;
}
.back-btn:hover { color: #00d4ff; }

.stock-id-label {
  font-size: 20px;
  font-weight: 800;
  color: #00d4ff;
  font-family: monospace;
  letter-spacing: 2px;
}
.stock-name-label {
  font-size: 14px;
  color: #5a7080;
  margin-left: 8px;
}

.ind-tag {
  font-size: 11px;
  padding: 2px 8px;
  background: rgba(0,212,255,0.06);
  border: 1px solid rgba(0,212,255,0.12);
  border-radius: 4px;
}
.ind-tag b { font-size: 13px; }

.up   { color: #ff4d6d; font-weight: 600; }
.down { color: #00ff88; font-weight: 600; }
.num  { color: #c8d6e5; }
</style>
