<template>
  <el-container class="layout">
    <!-- 側邊欄（桌面版） -->
    <el-aside width="220px" class="sidebar">
      <div class="logo">
        <span class="logo-icon">◈</span>
        <span class="logo-text">STOCK AI</span>
      </div>
      <el-menu :router="true" :default-active="$route.path" class="nav-menu">
        <el-menu-item index="/">
          <el-icon><Monitor /></el-icon>
          <span>儀表板</span>
        </el-menu-item>
        <el-menu-item index="/recommendations">
          <el-icon><TrendCharts /></el-icon>
          <span>選股結果</span>
        </el-menu-item>
        <el-menu-item index="/search">
          <el-icon><Search /></el-icon>
          <span>股票查詢</span>
        </el-menu-item>
        <el-menu-item index="/conferences">
          <el-icon><Calendar /></el-icon>
          <span>法說會行事曆</span>
        </el-menu-item>
        <el-menu-item index="/watchlist">
          <el-icon><Star /></el-icon>
          <span>觀察清單</span>
        </el-menu-item>
        <el-menu-item index="/us-market">
          <el-icon><DataLine /></el-icon>
          <span>全球市場</span>
        </el-menu-item>
      </el-menu>
      <div class="sidebar-footer">POWERED BY AI</div>
    </el-aside>

    <el-container>
      <el-header class="header">
        <div class="header-left">
          <span class="page-title">{{ $route.meta.title }}</span>
        </div>
        <div class="header-right">
          <span class="time">{{ currentTime }}</span>
        </div>
      </el-header>
      <el-main class="main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>

  <!-- 手機版底部導覽列 -->
  <nav class="mobile-nav">
    <router-link to="/"               class="mnav-item" active-class="mnav-active">
      <el-icon><Monitor /></el-icon><span>儀表板</span>
    </router-link>
    <router-link to="/recommendations" class="mnav-item" active-class="mnav-active">
      <el-icon><TrendCharts /></el-icon><span>選股</span>
    </router-link>
    <router-link to="/search"          class="mnav-item" active-class="mnav-active">
      <el-icon><Search /></el-icon><span>查詢</span>
    </router-link>
    <router-link to="/watchlist"       class="mnav-item" active-class="mnav-active">
      <el-icon><Star /></el-icon><span>觀察</span>
    </router-link>
    <router-link to="/us-market"       class="mnav-item" active-class="mnav-active">
      <el-icon><DataLine /></el-icon><span>全球</span>
    </router-link>
  </nav>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { Monitor, TrendCharts, Calendar, Star, Search, DataLine } from '@element-plus/icons-vue'

const currentTime = ref('')

function updateTime() {
  currentTime.value = new Date().toLocaleString('zh-TW', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  })
}

let timer
onMounted(() => { updateTime(); timer = setInterval(updateTime, 1000) })
onUnmounted(() => clearInterval(timer))
</script>

<style scoped>
.layout { height: 100vh; }

/* ── 側邊欄 ── */
.sidebar {
  background: #080c14;
  border-right: 1px solid rgba(0, 212, 255, 0.15);
  display: flex;
  flex-direction: column;
  position: relative;
  overflow: hidden;
}
.sidebar::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, #00d4ff, transparent);
}

.logo {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 24px 20px;
  border-bottom: 1px solid rgba(0, 212, 255, 0.12);
}
.logo-icon {
  font-size: 22px;
  color: #00d4ff;
  text-shadow: 0 0 12px #00d4ff;
  animation: pulse 2s ease-in-out infinite;
}
.logo-text {
  font-size: 16px;
  font-weight: 800;
  letter-spacing: 3px;
  color: #fff;
  background: linear-gradient(135deg, #00d4ff, #00ff88);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.nav-menu {
  background: transparent !important;
  border-right: none !important;
  flex: 1;
  padding: 8px 0;
}
:deep(.el-menu-item) {
  color: #5a7080 !important;
  height: 48px;
  margin: 2px 8px;
  border-radius: 8px;
  font-size: 13px;
  letter-spacing: 0.5px;
  transition: all 0.2s;
}
:deep(.el-menu-item:hover) {
  color: #00d4ff !important;
  background: rgba(0, 212, 255, 0.08) !important;
}
:deep(.el-menu-item.is-active) {
  color: #00d4ff !important;
  background: rgba(0, 212, 255, 0.12) !important;
  box-shadow: inset 3px 0 0 #00d4ff;
}

.sidebar-footer {
  padding: 16px 20px;
  font-size: 10px;
  letter-spacing: 2px;
  color: rgba(0, 212, 255, 0.3);
  border-top: 1px solid rgba(0, 212, 255, 0.08);
  text-align: center;
}

/* ── 頂部 ── */
.header {
  background: rgba(8, 12, 20, 0.95);
  border-bottom: 1px solid rgba(0, 212, 255, 0.1);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  backdrop-filter: blur(10px);
}
.page-title {
  font-size: 15px;
  font-weight: 700;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: #fff;
}
.time {
  font-size: 12px;
  font-family: 'Courier New', monospace;
  color: rgba(0, 212, 255, 0.6);
  letter-spacing: 1px;
}

/* ── 主內容 ── */
.main {
  background: #080c14;
  padding: 24px;
  overflow-y: auto;
}

@keyframes pulse {
  0%, 100% { text-shadow: 0 0 8px #00d4ff; }
  50%       { text-shadow: 0 0 20px #00d4ff, 0 0 40px #00d4ff; }
}

/* ── 手機版底部導覽列 ── */
.mobile-nav { display: none; }

@media (max-width: 768px) {
  /* 隱藏桌面側邊欄 */
  .sidebar { display: none !important; }

  /* 主內容滿版 */
  .main { padding: 12px; padding-bottom: 72px; }

  /* header 縮小 */
  .header { padding: 0 12px; }
  .time { display: none; }

  /* 底部導覽列 */
  .mobile-nav {
    display: flex;
    position: fixed;
    bottom: 0; left: 0; right: 0;
    height: 60px;
    background: #080c14;
    border-top: 1px solid rgba(0,212,255,0.15);
    z-index: 1000;
    justify-content: space-around;
    align-items: center;
    padding: 0 4px;
    backdrop-filter: blur(10px);
  }
}
</style>

<style>
/* 手機版導覽項目（非 scoped，因為 router-link 需要） */
@media (max-width: 768px) {
  .mnav-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 2px;
    padding: 6px 12px;
    color: #5a7080;
    text-decoration: none;
    font-size: 10px;
    letter-spacing: 0.5px;
    border-radius: 8px;
    transition: all 0.2s;
    flex: 1;
    justify-content: center;
  }
  .mnav-item .el-icon { font-size: 20px; }
  .mnav-item:hover { color: #00d4ff; }
  .mnav-item.mnav-active {
    color: #00d4ff;
    background: rgba(0,212,255,0.1);
  }
}
</style>
