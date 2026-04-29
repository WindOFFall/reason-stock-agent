import { createRouter, createWebHistory } from 'vue-router'
import Dashboard       from '../views/Dashboard.vue'
import Recommendations from '../views/Recommendations.vue'
import StockDetail     from '../views/StockDetail.vue'
import StockSearch     from '../views/StockSearch.vue'
import Conferences     from '../views/Conferences.vue'
import Watchlist       from '../views/Watchlist.vue'
import UsMarket        from '../views/UsMarket.vue'

const routes = [
  { path: '/',               component: Dashboard,       meta: { title: '儀表板' } },
  { path: '/recommendations', component: Recommendations, meta: { title: '選股結果' } },
  { path: '/search',         component: StockSearch,     meta: { title: '股票查詢' } },
  { path: '/stock/:id',      component: StockDetail,     meta: { title: '股票詳情' } },
  { path: '/conferences',    component: Conferences,     meta: { title: '法說會行事曆' } },
  { path: '/watchlist',      component: Watchlist,       meta: { title: '觀察清單' } },
  { path: '/us-market',      component: UsMarket,        meta: { title: '全球市場' } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.afterEach((to) => {
  document.title = `${to.meta.title || ''} | 股票 AI Agent`
})

export default router
