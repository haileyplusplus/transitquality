<template>
  <!-- from https://github.com/vuetifyjs/vuetify.git packages/docs/src/examples/v-autocomplete -->
  <v-app>
    <v-toolbar color="teal">
      <v-toolbar-title>Select a stop</v-toolbar-title>

      <v-autocomplete
        v-model="select"
        v-model:search="search"
        :items="items"
        :loading="loading"
        class="mx-4"
        density="comfortable"
        label="Pick a location"
        style="max-width: 300px;"
        hide-details
        hide-no-data
      />

      <v-btn icon="mdi-dots-vertical" />
    </v-toolbar>
    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>

<script setup>
  import { ref, watch } from 'vue'


  const loading = ref(false)
  const items = ref([])
  const search = ref(null)
  const select = ref(null)

  watch(search, val => {
    val && val !== select.value && querySelections(val)
  })

  async function querySelections (v) {
    console.log('query selections: ' + v)
    if (search.value) {
      loading.value = true
      const res = await fetch(
              `/api/search?text=` + search.value
          )
      const results = await res.json()
      if (results) {
        items.value = Object.keys(results.results)
        loading.value = false
      }
    }
  }
</script>

<script>
  export default {
    data () {
      return {
        loading: false,
        items: [],
        search: null,
        select: null,
      }
    },
    // watch: {
    //   search (val) {
    //     val && val !== this.select && this.querySelections(val)
    //   },
    // },
    // methods: {
    //   querySelections (v) {
    //     console.log('orig query selections: ' + v)
    //     this.items = []
    //     // this.loading = true
    //     // // Simulated ajax query
    //     // setTimeout(() => {
    //     //   this.items = []
    //     //   this.loading = false
    //     // }, 500)
    //   },
    // },
  }
</script>
