<template>
  <div>
    <!-- 分頁切換 -->
    <div class="market-tabs">
      <span class="tab" :class="{ active: tab === 'us' }"   @click="tab = 'us'">🇺🇸 美股</span>
      <span class="tab" :class="{ active: tab === 'kr' }"   @click="tab = 'kr'">🇰🇷 韓股</span>
      <span class="tab" :class="{ active: tab === 'jp' }"   @click="tab = 'jp'">🇯🇵 日股</span>
    </div>

    <div v-if="loading" style="text-align:center;padding:60px;color:#5a7080">載入中...</div>

    <!-- 美股 -->
    <template v-if="!loading && tab === 'us'">
      <el-row :gutter="12" style="margin-bottom:20px">
        <el-col v-for="idx in usData.indices" :key="idx.ticker"
          :span="Math.floor(24 / (usData.indices.length || 1))">
          <MarketCard :item="idx" :chartKey="'us_idx_' + idx.ticker" :setRef="setRef" />
        </el-col>
      </el-row>
      <div class="section-title">個股</div>
      <div class="stock-grid">
        <MarketCard v-for="s in usData.stocks" :key="s.ticker"
          :item="s" :chartKey="'us_' + s.ticker" :setRef="setRef" />
      </div>
    </template>

    <!-- 韓股 -->
    <template v-if="!loading && tab === 'kr'">
      <el-row :gutter="12" style="margin-bottom:20px">
        <el-col v-for="idx in asiaData.korea.filter(i => i.is_index)" :key="idx.ticker"
          :span="12">
          <MarketCard :item="idx" :chartKey="'kr_idx_' + idx.ticker" :setRef="setRef" />
        </el-col>
      </el-row>
      <div class="section-title">個股</div>
      <div class="stock-grid">
        <MarketCard v-for="s in asiaData.korea.filter(i => !i.is_index)" :key="s.ticker"
          :item="s" :chartKey="'kr_' + s.ticker" :setRef="setRef" />
      </div>
    </template>

    <!-- 日股 -->
    <template v-if="!loading && tab === 'jp'">
      <el-row :gutter="12" style="margin-bottom:20px">
        <el-col v-for="idx in asiaData.japan.filter(i => i.is_index)" :key="idx.ticker"
          :span="12">
          <MarketCard :item="idx" :chartKey="'jp_idx_' + idx.ticker" :setRef="setRef" />
        </el-col>
      </el-row>
      <div class="section-title">個股</div>
      <div class="stock-grid">
        <MarketCard v-for="s in asiaData.japan.filter(i => !i.is_index)" :key="s.ticker"
          :item="s" :chartKey="'jp_' + s.ticker" :setRef="setRef" />
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getUsOverview, getAsiaOverview } from '../api/index.js'

const tab     = ref('us')
const loading = ref(true)
const usData  = reactive({ stocks: [], indices: [] })
const asiaData = reactive({ korea: [], japan: [] })
const domRefs = {}
const chartInstances = {}

const US_STOCK_NAMES = {
  AAPL: 'Apple', AMD: 'AMD', AMZN: 'Amazon', GOOGL: 'Google',
  INTC: 'Intel', META: 'Meta', MSFT: 'Microsoft',
  NVDA: 'NVIDIA', TSLA: 'Tesla', TSM: '台積電 ADR',
  '^GSPC': 'S&P 500', '^IXIC': 'NASDAQ', '^DJI': '道瓊', '^SOX': '費城半導體', '^TWII': '台灣加權',
}

function setRef(el, key) {
  if (!el) {
    if (chartInstances[key]) {
      chartInstances[key].dispose()
      delete chartInstances[key]
    }
    delete domRefs[key]
  } else {
    domRefs[key] = el
  }
}

function drawSpark(key, closes, isUp, dates = []) {
  const el = domRefs[key]
  if (!el) return
  if (!chartInstances[key]) chartInstances[key] = echarts.init(el)
  const chart = chartInstances[key]
  const color = isUp ? '#ff4d6d' : '#00ff88'
  chart.setOption({
    backgroundColor: 'transparent',
    grid: { left: 0, right: 0, top: 4, bottom: 0 },
    xAxis: { type: 'category', show: false, data: dates },
    yAxis: { type: 'value', show: false, scale: true },
    series: [{
      type: 'line', data: closes, smooth: true, symbol: 'none',
      lineStyle: { width: 1.5, color },
      areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
        { offset: 0, color: color + '44' }, { offset: 1, color: 'transparent' },
      ])},
    }],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#0d1220', borderColor: 'rgba(0,212,255,0.2)',
      textStyle: { color: '#c8d6e5', fontSize: 11 },
      formatter(p) {
        return `<div style="color:#5a7080;font-size:10px">${p[0].axisValue}</div>
                <div style="font-weight:700">${p[0].value?.toLocaleString()}</div>`
      },
    },
  })
}

function drawAll() {
  nextTick(() => {
    const items = tab.value === 'us'
      ? [...usData.indices.map(i => ({ ...i, key: 'us_idx_' + i.ticker })),
         ...usData.stocks.map(s => ({ ...s, key: 'us_' + s.ticker }))]
      : tab.value === 'kr'
      ? asiaData.korea.map(i => ({ ...i, key: (i.is_index ? 'kr_idx_' : 'kr_') + i.ticker }))
      : asiaData.japan.map(i => ({ ...i, key: (i.is_index ? 'jp_idx_' : 'jp_') + i.ticker }))

    for (const item of items) {
      drawSpark(item.key, item.closes, item.change >= 0, item.dates)
    }
  })
}

watch(tab, drawAll)

onMounted(async () => {
  const [us, asia] = await Promise.allSettled([getUsOverview(), getAsiaOverview()])
  if (us.status === 'fulfilled') {
    usData.stocks  = us.value.data.stocks.map(s => ({ ...s, name: US_STOCK_NAMES[s.ticker] || s.ticker }))
    usData.indices = us.value.data.indices.map(s => ({ ...s, name: US_STOCK_NAMES[s.ticker] || s.ticker }))
  }
  if (asia.status === 'fulfilled') {
    asiaData.korea = asia.value.data.korea
    asiaData.japan = asia.value.data.japan
  }
  loading.value = false
  drawAll()
})
</script>

<script>
// MarketCard 子元件（inline 定義）
import { defineComponent, h, onMounted } from 'vue'

export const MarketCard = defineComponent({
  props: ['item', 'chartKey', 'setRef'],
  setup(props) {
    return () => {
      const s = props.item
      const isUp = s.change >= 0
      const color = isUp ? '#ff4d6d' : '#00ff88'
      return h('div', {
        class: ['stock-card', isUp ? 'card-up' : 'card-down'],
      }, [
        h('div', { class: 'card-header' }, [
          h('div', {}, [
            h('div', { class: 'ticker' }, s.ticker),
            h('div', { class: 'sname' }, s.name || ''),
          ]),
          h('div', { class: 'card-right' }, [
            h('div', { class: 's-price' }, s.latest?.toLocaleString()),
            h('div', { style: { color, fontSize: '12px', fontWeight: 600, marginTop: '2px' } },
              `${isUp ? '+' : ''}${s.change_pct}%`),
          ]),
        ]),
        h('div', {
          ref: el => props.setRef(el, props.chartKey),
          class: 'spark',
        }),
      ])
    }
  },
})
</script>

<style scoped>
.market-tabs {
  display: flex;
  gap: 4px;
  background: rgba(0,212,255,0.05);
  border: 1px solid rgba(0,212,255,0.12);
  border-radius: 8px;
  padding: 4px;
  width: fit-content;
  margin-bottom: 20px;
}
.tab {
  padding: 6px 20px;
  font-size: 13px;
  font-weight: 600;
  color: #5a7080;
  cursor: pointer;
  border-radius: 6px;
  letter-spacing: 0.5px;
  transition: all 0.2s;
  user-select: none;
}
.tab:hover  { color: #00d4ff; }
.tab.active { background: rgba(0,212,255,0.15); color: #00d4ff; }

.section-title {
  font-size: 11px;
  letter-spacing: 2px;
  color: #5a7080;
  text-transform: uppercase;
  margin-bottom: 12px;
}
.stock-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 12px;
}
</style>

<style>
.stock-card {
  background: #0d1220;
  border: 1px solid rgba(0,212,255,0.1);
  border-radius: 12px;
  padding: 14px 14px 8px;
  transition: border-color 0.25s, box-shadow 0.25s;
}
.stock-card:hover {
  border-color: rgba(0,212,255,0.3);
  box-shadow: 0 0 16px rgba(0,212,255,0.06);
}
.card-up   { border-left: 3px solid rgba(255,77,109,0.5); }
.card-down { border-left: 3px solid rgba(0,255,136,0.5); }
.card-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; }
.ticker  { font-size: 13px; font-weight: 800; color: #00d4ff; font-family: monospace; letter-spacing: 1px; }
.sname   { font-size: 11px; color: #5a7080; margin-top: 2px; }
.card-right { text-align: right; }
.s-price { font-size: 15px; font-weight: 700; color: #c8d6e5; }
.spark   { height: 52px; margin-top: 4px; }
</style>
