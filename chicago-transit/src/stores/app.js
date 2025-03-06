// Utilities
import { defineStore } from 'pinia'

export const useAppStore = defineStore('app', {
  state: () => {
    return {
      currentDirection: [],
      summaries: {n: [], w: [], e: [], s:[]}
    }
  },
})
