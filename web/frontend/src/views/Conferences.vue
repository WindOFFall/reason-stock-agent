<template>
  <div>
    <!-- 工具列 -->
    <div class="toolbar">
      <div class="toolbar-left">
        <el-input v-model="search" placeholder="搜尋代號 / 公司名稱" clearable style="width:220px" />
        <el-select v-model="marketFilter" style="width:120px">
          <el-option label="全部市場" value="" />
          <el-option label="上市" value="上市" />
          <el-option label="上櫃" value="上櫃" />
        </el-select>
      </div>
      <div class="toolbar-right">
        <span class="count-badge">共 <b>{{ filtered.length }}</b> 場即將召開</span>
      </div>
    </div>

    <el-card>
      <el-table :data="filtered" v-loading="loading" stripe @row-click="toDetail" style="cursor:pointer">
        <el-table-column prop="conf_date" label="日期" width="120">
          <template #default="{ row }">
            <span :class="isToday(row.conf_date) ? 'today' : isThisWeek(row.conf_date) ? 'this-week' : ''">
              {{ row.conf_date }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="conf_time" label="時間" width="90">
          <template #default="{ row }">
            <span class="time-text">{{ row.conf_time || '—' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="market" label="市場" width="80">
          <template #default="{ row }">
            <el-tag :type="row.market === '上市' ? 'info' : 'success'" size="small">
              {{ row.market }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="stock_id" label="代號" width="100">
          <template #default="{ row }">
            <span class="stock-id">{{ row.stock_id }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="name" label="公司名稱" />
        <el-table-column label="" width="60" align="center">
          <template #default>
            <span class="arrow">›</span>
          </template>
        </el-table-column>
      </el-table>

      <div v-if="!loading && !filtered.length">
        <el-empty description="暫無法說會資料" />
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { getConferences } from '../api/index.js'

const router       = useRouter()
const list         = ref([])
const loading      = ref(true)
const search       = ref('')
const marketFilter = ref('')

const today = new Date().toISOString().slice(0, 10)

const filtered = computed(() => {
  const q = search.value.toLowerCase()
  return list.value.filter(r => {
    const matchSearch = !q || r.stock_id?.includes(q) || r.name?.toLowerCase().includes(q)
    const matchMarket = !marketFilter.value || r.market === marketFilter.value
    return matchSearch && matchMarket
  })
})

function isToday(d) { return d === today }

function isThisWeek(d) {
  const diff = (new Date(d) - new Date(today)) / 86400000
  return diff >= 0 && diff <= 7
}

function toDetail(row) {
  router.push(`/stock/${row.stock_id}`)
}

onMounted(async () => {
  const res = await getConferences(100)
  list.value    = res.data
  loading.value = false
})
</script>

<style scoped>
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  gap: 12px;
  flex-wrap: wrap;
}
.toolbar-left  { display: flex; gap: 10px; flex-wrap: wrap; }
.count-badge   { font-size: 12px; color: #5a7080; }
.count-badge b { color: #00d4ff; font-size: 14px; }

.stock-id  { font-family: monospace; font-weight: 700; color: #00d4ff; font-size: 13px; }
.time-text { font-family: monospace; color: #5a7080; font-size: 12px; }
.arrow     { color: rgba(0,212,255,0.4); font-size: 18px; }

.today     { color: #f6c90e; font-weight: 700; }
.this-week { color: #00d4ff; font-weight: 600; }
</style>
