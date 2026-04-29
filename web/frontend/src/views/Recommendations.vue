<template>
  <div>
    <!-- 篩選工具列 -->
    <div class="toolbar">
      <div class="toolbar-left">
        <el-input
          v-model="search"
          placeholder="搜尋代號 / 公司名稱"
          clearable
          style="width: 220px"
        />
        <el-select v-model="limitVal" style="width: 120px">
          <el-option label="最近 20 筆" :value="20" />
          <el-option label="最近 50 筆" :value="50" />
          <el-option label="最近 100 筆" :value="100" />
        </el-select>
        <el-button type="primary" @click="load">重新載入</el-button>
      </div>
      <div class="toolbar-right">
        <span class="count-badge">共 <b>{{ filtered.length }}</b> 筆選股訊號</span>
      </div>
    </div>

    <!-- 主表格 -->
    <el-card>
      <el-table
        :data="filtered"
        v-loading="loading"
        stripe
        :row-class-name="rowClass"
        @row-click="toDetail"
        style="cursor: pointer"
      >
        <el-table-column prop="date" label="日期" width="120" />
        <el-table-column prop="stock_id" label="代號" width="90">
          <template #default="{ row }">
            <span class="stock-id">{{ row.stock_id }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="name" label="公司" width="120" />
        <el-table-column prop="action" label="訊號" width="90">
          <template #default="{ row }">
            <el-tag
              :type="row.action === 'BUY' ? 'success' : row.action === 'WATCH' ? 'warning' : 'info'"
              size="small"
            >
              {{ row.action === 'BUY' ? '買進' : row.action === 'WATCH' ? '觀望' : '不買' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="entry_price" label="進場價" width="100">
          <template #default="{ row }">
            <span class="price">{{ row.entry_price?.toLocaleString() }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="reason" label="選股理由" show-overflow-tooltip />
        <el-table-column label="" width="60" align="center">
          <template #default>
            <span class="arrow">›</span>
          </template>
        </el-table-column>
      </el-table>

      <div v-if="!loading && !filtered.length" class="empty-hint">
        <el-empty description="暫無選股訊號" />
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { getRecommendations } from '../api/index.js'

const router   = useRouter()
const list     = ref([])
const loading  = ref(true)
const search   = ref('')
const limitVal = ref(20)

const filtered = computed(() => {
  const q = search.value.toLowerCase()
  return list.value.filter(r =>
    !q || r.stock_id?.includes(q) || r.name?.toLowerCase().includes(q)
  )
})

async function load() {
  loading.value = true
  const res = await getRecommendations(limitVal.value)
  list.value    = res.data
  loading.value = false
}

function rowClass() { return 'rec-row' }

function toDetail(row) {
  router.push(`/stock/${row.stock_id}`)
}

onMounted(load)
</script>

<style scoped>
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  flex-wrap: wrap;
  gap: 12px;
}
.toolbar-left {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.count-badge {
  font-size: 12px;
  color: #5a7080;
  letter-spacing: 0.5px;
}
.count-badge b {
  color: #00d4ff;
  font-size: 14px;
}
.stock-id {
  font-family: monospace;
  font-weight: 700;
  color: #00d4ff;
  font-size: 13px;
}
.price {
  font-weight: 600;
  color: #f6c90e;
}
.arrow {
  color: rgba(0,212,255,0.4);
  font-size: 18px;
  line-height: 1;
}
:deep(.rec-row:hover .arrow) {
  color: #00d4ff;
}
</style>
