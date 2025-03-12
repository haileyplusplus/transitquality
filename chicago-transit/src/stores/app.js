// Utilities
import { defineStore } from 'pinia'

export const useAppStore = defineStore('app', {
  state: () => {
    return {
      currentDirection: null,
      summaries: {n: [], w: [], e: [], s:[]},
      currentDetail: {}
    }
  },
})
