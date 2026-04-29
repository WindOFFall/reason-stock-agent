<template>
  <div>
    <el-card style="margin-bottom: 16px">
      <el-row :gutter="12" align="middle">
        <el-col :span="6">
          <el-input
            v-model="inputId"
            placeholder="輸入股票代號，如 2330"
            clearable
            @keyup.enter="search"
          />
        </el-col>
        <el-col :span="4">
          <el-select v-model="days" style="width:100%">
            <el-option label="近 30 天"  :value="30" />
            <el-option label="近 60 天"  :value="60" />
            <el-option label="近 120 天" :value="120" />
          </el-select>
        </el-col>
        <el-col :span="3">
          <el-button type="primary" @click="search" :loading="loading">查詢</el-button>
        </el-col>
        <el-col :span="8">
          <span v-if="stockId" style="font-size:18px; font-weight:bold; color:#333">
            {{ stockId }}
          </span>
          <!-- 最新指標摘要 -->
          <el-tag v-if="latestInd" style="margin-left:12px">
            RSI {{ latestInd.rsi?.toFixed(1) }}
          </el-tag>
          <el-tag v-if="latestInd" type="success" style="margin-left:6px">
            量比 {{ latestInd.vol_ratio }}x
          </el-tag>
        </el-col>
      </el-row>
    </el-card>

    <template v-if="stockId">
      <!-- K 線 + MA + 成交量 + RSI -->
      <el-card header="股價走勢與技術指標" style="margin-bottom: 16px">
        <div ref="chartRef" style="height: 560px" />
      </el-card>

      <!-- 法人買賣 -->
      <el-card header="三大法人（近期）" style="margin-bottom: 16px">
        <el-table :data="instData.slice().reverse()" size="small" height="260">
          <el-table-column prop="date"        label="日期"      width="120" />
          <el-table-column prop="foreign_net" label="外資（張）">
            <template #default="{ row }">
              <span :style="{ color: row.foreign_net > 0 ? '#f56c6c' : '#67c23a' }">
                {{ row.foreign_net?.toLocaleString() }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="invest_net" label="投信（張）">
            <template #default="{ row }">
              <span :style="{ color: row.invest_net > 0 ? '#f56c6c' : '#67c23a' }">
                {{ row.invest_net?.toLocaleString() }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="dealer_net" label="自營商（張）">
            <template #default="{ row }">
              <span :style="{ color: row.dealer_net > 0 ? '#f56c6c' : '#67c23a' }">
                {{ row.dealer_net?.toLocaleString() }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="total" label="三大合計（張）">
            <template #default="{ row }">
              <span :style="{ color: row.total > 0 ? '#f56c6c' : '#67c23a' }">
                {{ row.total?.toLocaleString() }}
              </span>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- EPS -->
      <el-card header="近 8 季 EPS">
        <el-table :data="epsData" size="small">
          <el-table-column prop="date"  label="日期"  width="120" />
          <el-table-column prop="value" label="EPS">
            <template #default="{ row }">
              <span :style="{ color: row.value > 0 ? '#f56c6c' : '#67c23a' }">
                {{ row.value }}
              </span>
            </template>
          </el-table-column>
        </el-table>
      </el-card>
    </template>

    <el-empty v-else description="請輸入股票代號查詢" />
  </div>
</template>

<script setup>
import { ref, nextTick, computed } from 'vue'
import * as echarts from 'echarts'
import { getStockPrice, getInstitutional, getEps, getIndicators } from '../api/index.js'

const inputId   = ref('')
const stockId   = ref('')
const days      = ref(60)
const loading   = ref(false)
const instData  = ref([])
const epsData   = ref([])
const indData   = ref([])
const chartRef  = ref(null)
let chart       = null

const latestInd = computed(() => indData.value.at(-1) ?? null)

async function search() {
  if (!inputId.value.trim()) return
  stockId.value = inputId.value.trim()
  loading.value = true

  const [p, i, e, ind] = await Promise.allSettled([
    getStockPrice(stockId.value, days.value),
    getInstitutional(stockId.value, days.value),
    getEps(stockId.value),
    getIndicators(stockId.value, days.value),
  ])

  instData.value = i.status === 'fulfilled' ? i.value.data : []
  epsData.value  = e.status === 'fulfilled' ? e.value.data : []
  indData.value  = ind.status === 'fulfilled' ? ind.value.data : []

  await nextTick()
  if (p.status === 'fulfilled') drawChart(p.value.data, indData.value)
  loading.value = false
}

function drawChart(priceData, indArr) {
  if (!chartRef.value) return
  if (!chart) chart = echarts.init(chartRef.value)

  const dates   = priceData.map(d => d.date)
  const candles = priceData.map(d => [d.open, d.close, d.low, d.high])
  const volumes = priceData.map(d => d.volume)

  // 對齊指標資料到 dates
  const indMap  = Object.fromEntries(indArr.map(d => [d.date, d]))
  const ma5     = dates.map(d => indMap[d]?.ma5?.toFixed(2) ?? null)
  const ma20    = dates.map(d => indMap[d]?.ma20?.toFixed(2) ?? null)
  const ma60    = dates.map(d => indMap[d]?.ma60?.toFixed(2) ?? null)
  const rsi     = dates.map(d => indMap[d]?.rsi?.toFixed(1) ?? null)

  chart.setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['K線', 'MA5', 'MA20', 'MA60'], top: 4 },
    grid: [
      { left: 60, right: 20, top: 36, height: '45%' },
      { left: 60, right: 20, top: '57%', height: '15%' },
      { left: 60, right: 20, top: '76%', height: '18%' },
    ],
    xAxis: [
      { type: 'category', data: dates, gridIndex: 0, axisLabel: { fontSize: 11 } },
      { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: false } },
      { type: 'category', data: dates, gridIndex: 2, axisLabel: { fontSize: 10 } },
    ],
    yAxis: [
      { type: 'value', gridIndex: 0, scale: true, splitNumber: 4 },
      { type: 'value', gridIndex: 1, splitNumber: 2 },
      { type: 'value', gridIndex: 2, min: 0, max: 100, splitNumber: 2 },
    ],
    series: [
      {
        name: 'K線', type: 'candlestick',
        xAxisIndex: 0, yAxisIndex: 0,
        data: candles,
        itemStyle: {
          color: '#f56c6c', color0: '#67c23a',
          borderColor: '#f56c6c', borderColor0: '#67c23a',
        },
      },
      { name: 'MA5',  type: 'line', xAxisIndex: 0, yAxisIndex: 0, data: ma5,  smooth: true, symbol: 'none', lineStyle: { width: 1.5, color: '#e6a23c' } },
      { name: 'MA20', type: 'line', xAxisIndex: 0, yAxisIndex: 0, data: ma20, smooth: true, symbol: 'none', lineStyle: { width: 1.5, color: '#409eff' } },
      { name: 'MA60', type: 'line', xAxisIndex: 0, yAxisIndex: 0, data: ma60, smooth: true, symbol: 'none', lineStyle: { width: 1.5, color: '#9b59b6' } },
      {
        type: 'bar', xAxisIndex: 1, yAxisIndex: 1,
        data: volumes,
        itemStyle: { color: '#909399' },
      },
      {
        name: 'RSI', type: 'line', xAxisIndex: 2, yAxisIndex: 2,
        data: rsi, smooth: true, symbol: 'none',
        lineStyle: { width: 1.5, color: '#f39c12' },
        markLine: {
          silent: true,
          data: [
            { yAxis: 70, lineStyle: { color: '#f56c6c', type: 'dashed' } },
            { yAxis: 30, lineStyle: { color: '#67c23a', type: 'dashed' } },
          ],
        },
      },
    ],
  })
}
</script>
