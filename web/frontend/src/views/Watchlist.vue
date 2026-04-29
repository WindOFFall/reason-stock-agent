<template>
  <div>
    <div class="toolbar">
      <div class="toolbar-left">
        <el-input v-model="search" placeholder="搜尋代號 / 公司名稱" clearable style="width:220px" />
      </div>
      <div class="toolbar-right">
        <span class="count-badge">追蹤中 <b>{{ filtered.length }}</b> 檔</span>
      </div>
    </div>

    <el-card>
      <el-empty v-if="!loading && !list.length" description="觀察清單目前無持倉" />
      <el-table
        v-else
        :data="filtered"
        v-loading="loading"
        stripe
        @row-click="toDetail"
        style="cursor:pointer"
      >
        <el-table-column prop="date" label="加入日期" width="120" />
        <el-table-column prop="stock_id" label="代號" width="90">
          <template #default="{ row }">
            <span class="stock-id">{{ row.stock_id }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="name" label="公司" width="130" />
        <el-table-column prop="action" label="操作" width="80">
          <template #default="{ row }">
            <el-tag type="success" size="small">{{ row.action }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="entry_price" label="進場價" width="100">
          <template #default="{ row }">
            <span class="price">{{ row.entry_price?.toLocaleString() ?? '—' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="reason" label="理由" show-overflow-tooltip />
        <el-table-column label="" width="60" align="center">
          <template #default>
            <span class="arrow">›</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { getWatchlist } from '../api/index.js'

const router  = useRouter()
const list    = ref([])
const loading = ref(true)
const search  = ref('')

const filtered = computed(() => {
  const q = search.value.toLowerCase()
  return list.value.filter(r =>
    !q || r.stock_id?.includes(q) || r.name?.toLowerCase().includes(q)
  )
})

function toDetail(row) {
  router.push(`/stock/${row.stock_id}`)
}

onMounted(async () => {
  const res = await getWatchlist()
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
}
.toolbar-left  { display: flex; gap: 10px; }
.count-badge   { font-size: 12px; color: #5a7080; }
.count-badge b { color: #00d4ff; font-size: 14px; }

.stock-id { font-family: monospace; font-weight: 700; color: #00d4ff; font-size: 13px; }
.price    { font-weight: 600; color: #f6c90e; }
.arrow    { color: rgba(0,212,255,0.4); font-size: 18px; }
</style>
