<template>
  <div>
    <!-- 狀態卡片 -->
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :xs="12" :span="6">
        <div class="stat-card">
          <div class="stat-label">資料日期</div>
          <div class="stat-value date">{{ status.date || '---' }}</div>
        </div>
      </el-col>
      <el-col :xs="12" :span="6">
        <div class="stat-card" :class="status.price ? 'ok' : 'fail'">
          <div class="stat-label">股價資料</div>
          <div class="stat-value">{{ status.price ? '✓ 完整' : '✗ 缺漏' }}</div>
          <div class="stat-glow" />
        </div>
      </el-col>
      <el-col :xs="12" :span="6">
        <div class="stat-card" :class="status.inst ? 'ok' : 'fail'">
          <div class="stat-label">三大法人</div>
          <div class="stat-value">{{ status.inst ? '✓ 完整' : '✗ 缺漏' }}</div>
          <div class="stat-glow" />
        </div>
      </el-col>
      <el-col :xs="12" :span="6">
        <div class="stat-card" :class="status.news ? 'ok' : 'fail'">
          <div class="stat-label">新聞資料</div>
          <div class="stat-value">{{ status.news ? '✓ 完整' : '✗ 缺漏' }}</div>
          <div class="stat-glow" />
        </div>
      </el-col>
    </el-row>

    <!-- 大盤指數走勢 -->
    <el-card style="margin-bottom: 20px">
      <template #header>
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px">
          <span>大盤指數走勢</span>
          <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap">
            <!-- 時間範圍選擇 -->
            <div class="range-btns">
              <span v-for="r in ranges" :key="r.days"
                class="range-btn" :class="{ active: activeDays === r.days }"
                @click="changeRange(r.days)">{{ r.label }}</span>
            </div>
            <!-- 最新報價 -->
            <div style="display:flex; gap:16px; font-size:12px; flex-wrap:wrap">
              <span v-for="(info, name) in indexSummary" :key="name">
                <span style="color:#5a7080">{{ name }}</span>
                <span :class="info.change >= 0 ? 'up' : 'down'" style="margin-left:6px; font-weight:700">
                  {{ info.latest?.toLocaleString() }}
                  <span style="font-size:11px">({{ info.change >= 0 ? '+' : '' }}{{ info.change }}%)</span>
                </span>
              </span>
            </div>
          </div>
        </div>
      </template>
      <div style="position:relative; height:260px">
        <div v-if="loadingIndex" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#5a7080;font-size:13px;z-index:1">載入中...</div>
        <div ref="indexChartRef" style="height:260px" />
      </div>
      <div style="font-size:10px;color:#5a7080;margin-top:4px;text-align:right">
        <span v-for="(arr, name) in indexData" :key="name" style="margin-left:12px">
          {{ name }}: {{ arr[0]?.date }} ~ {{ arr[arr.length-1]?.date }}
        </span>
      </div>
    </el-card>

    <!-- 法人 + 法說會 -->
    <el-row :gutter="16">
      <el-col :xs="24" :span="12">
        <el-card header="外資買超 Top 10">
          <el-table :data="topInst" size="small" height="280">
            <el-table-column prop="stock_id"    label="代號"   width="90" />
            <el-table-column prop="foreign_net" label="外資（張）">
              <template #default="{ row }">
                <span :class="row.foreign_net > 0 ? 'up' : 'down'">
                  {{ row.foreign_net?.toLocaleString() }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="invest_net" label="投信（張）">
              <template #default="{ row }">
                <span :class="row.invest_net > 0 ? 'up' : 'down'">
                  {{ row.invest_net?.toLocaleString() }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="" width="70">
              <template #default="{ row }">
                <span class="link" @click="$router.push(`/stock/${row.stock_id}`)">查看</span>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
      <el-col :xs="24" :span="12">
        <el-card header="即將召開法說會">
          <el-table :data="conferences" size="small" height="280">
            <el-table-column prop="conf_date" label="日期"  width="110" />
            <el-table-column prop="stock_id"  label="代號"  width="80" />
            <el-table-column prop="name"      label="公司" />
            <el-table-column prop="conf_time" label="時間"  width="75" />
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getStatus, getInstitutionalTop, getConferences, getMarketIndex, getAllIndices } from '../api/index.js'

const status        = ref({})
const topInst       = ref([])
const conferences   = ref([])
const indexData     = ref({})
const indexChartRef = ref(null)
const loadingIndex  = ref(false)
const activeDays    = ref(60)
let indexChart      = null

// 四個市場各取一個代表指數
const SHOW_KEYS = ['台灣加權', '費城半導體', 'KOSPI', '日經225']

const ranges = [
  { label: '1M',  days: 30  },
  { label: '3M',  days: 90  },
  { label: '6M',  days: 180 },
  { label: '1Y',  days: 365 },
  { label: '2Y',  days: 730 },
]

const indexSummary = computed(() => {
  const result = {}
  for (const [name, arr] of Object.entries(indexData.value)) {
    if (!arr?.length) continue
    const latest = arr.at(-1)?.close
    const prev   = arr.at(-2)?.close
    const change = prev ? (((latest - prev) / prev) * 100).toFixed(2) : 0
    result[name] = { latest, change: parseFloat(change) }
  }
  return result
})

function drawIndexChart(data) {
  if (!indexChartRef.value) return
  if (!indexChart) indexChart = echarts.init(indexChartRef.value)

  const colors  = ['#00d4ff', '#ff4d6d', '#00ff88', '#f6c90e']
  const rawMap  = {}

  // 正規化成 % 漲跌幅，用 [date, value] pair 讓各市場對齊自己的交易日
  const series = Object.entries(data).map(([name, arr], i) => {
    const base = arr[0]?.close || 1
    rawMap[name] = {}
    return {
      name,
      type: 'line',
      data: arr.map(d => {
        rawMap[name][d.date] = d.close
        return [d.date, parseFloat(((d.close - base) / base * 100).toFixed(2))]
      }),
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 2, color: colors[i % colors.length] },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: colors[i % colors.length] + '22' },
          { offset: 1, color: 'transparent' },
        ]),
      },
    }
  })

  indexChart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#0d1220',
      borderColor: 'rgba(0,212,255,0.2)',
      textStyle: { color: '#c8d6e5', fontSize: 12 },
      formatter(params) {
        const ts   = params[0]?.axisValue
        const date = ts ? new Date(ts).toISOString().slice(0, 10) : ''
        let html = `<div style="color:#5a7080;font-size:11px;margin-bottom:4px">${date}</div>`
        for (const p of params) {
          const raw  = rawMap[p.seriesName]?.[date]
          const sign = p.value >= 0 ? '+' : ''
          const col  = p.value >= 0 ? '#ff4d6d' : '#00ff88'
          html += `<div style="display:flex;justify-content:space-between;gap:20px;line-height:1.9">
            <span style="color:${p.color}">● ${p.seriesName}</span>
            <span>
              <b>${raw?.toLocaleString() ?? '—'}</b>
              <span style="color:${col};font-size:11px;margin-left:6px">${sign}${p.value}%</span>
            </span>
          </div>`
        }
        return html
      },
    },
    legend: { textStyle: { color: '#5a7080', fontSize: 11 }, top: 0 },
    grid: { left: 55, right: 20, top: 30, bottom: 20 },
    xAxis: { type: 'time', axisLabel: { color: '#5a7080', fontSize: 11 }, axisLine: { lineStyle: { color: 'rgba(0,212,255,0.1)' } }, splitLine: { show: false } },
    yAxis: {
      type: 'value', scale: true,
      axisLabel: { color: '#5a7080', fontSize: 11, formatter: v => v + '%' },
      splitLine: { lineStyle: { color: 'rgba(0,212,255,0.05)' } },
    },
    series,
  }, true)
}

async function loadAllIndices() {
  loadingIndex.value = true
  const [tw, all] = await Promise.all([
    getMarketIndex(activeDays.value),
    getAllIndices(activeDays.value),
  ])
  // 合併台股 + 全球指數，只留需要的四條
  const merged = { ...tw.data, ...all.data }
  const filtered = {}
  for (const key of SHOW_KEYS) {
    if (merged[key]) filtered[key] = merged[key]
  }
  indexData.value    = filtered
  loadingIndex.value = false
  await nextTick()
  drawIndexChart(filtered)
}

async function changeRange(days) {
  activeDays.value = days
  await loadAllIndices()
}

onMounted(async () => {
  const [s, t, c] = await Promise.all([
    getStatus(), getInstitutionalTop(10), getConferences(5),
  ])
  status.value      = s.data
  topInst.value     = t.data
  conferences.value = c.data
  await loadAllIndices()
})
</script>

<style scoped>
.stat-card {
  background: #0d1220;
  border: 1px solid rgba(0, 212, 255, 0.12);
  border-radius: 12px;
  padding: 20px;
  position: relative;
  overflow: hidden;
  transition: border-color 0.3s, box-shadow 0.3s;
}
.stat-card:hover {
  border-color: rgba(0, 212, 255, 0.3);
  box-shadow: 0 0 20px rgba(0, 212, 255, 0.06);
}
.stat-label {
  font-size: 11px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  color: #5a7080;
  margin-bottom: 10px;
}
.stat-value {
  font-size: 22px;
  font-weight: 700;
  letter-spacing: 1px;
  color: #c8d6e5;
}
.stat-value.date { font-size: 17px; color: rgba(0, 212, 255, 0.8); font-family: monospace; }
.stat-glow {
  position: absolute;
  bottom: -20px; right: -20px;
  width: 80px; height: 80px;
  border-radius: 50%;
  filter: blur(30px);
  opacity: 0.15;
}
.ok .stat-value  { color: #00ff88; }
.ok .stat-glow   { background: #00ff88; }
.fail .stat-value { color: #ff4d6d; }
.fail .stat-glow  { background: #ff4d6d; }

.up   { color: #ff4d6d; font-weight: 600; }
.down { color: #00ff88; font-weight: 600; }
.link { color: rgba(0,212,255,0.7); cursor: pointer; font-size: 12px; }
.link:hover { color: #00d4ff; text-decoration: underline; }

.range-btns {
  display: flex;
  gap: 4px;
  background: rgba(0,212,255,0.05);
  border: 1px solid rgba(0,212,255,0.12);
  border-radius: 6px;
  padding: 2px;
}
.range-btn {
  padding: 2px 10px;
  font-size: 11px;
  font-weight: 600;
  color: #5a7080;
  cursor: pointer;
  border-radius: 4px;
  letter-spacing: 0.5px;
  transition: all 0.2s;
  user-select: none;
}
.range-btn:hover { color: #00d4ff; }
.range-btn.active {
  background: rgba(0,212,255,0.15);
  color: #00d4ff;
}

@media (max-width: 768px) {
  .stat-card { padding: 14px; margin-bottom: 8px; }
  .stat-value { font-size: 17px; }
  .stat-value.date { font-size: 13px; }
}
</style>
