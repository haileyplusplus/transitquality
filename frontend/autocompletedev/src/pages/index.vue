<template>
  <v-toolbar color="teal">
    <v-toolbar-title>Select a stop</v-toolbar-title>

    <v-autocomplete
      v-model="select"
      v-model:search="search"
      :items="items"
      :loading="loading"
      class="mx-4"
      density="comfortable"
      label="Select destination"
      style="max-width: 300px;"
      hide-details
      hide-no-data
    />

    <v-btn icon="mdi-dots-vertical" />
  </v-toolbar>

  Main vue<br>
  Str: {{ userlocation }}, set {{ locationset }}

  Trip info: {{ tripinfo }}
</template>

<script setup>
  import { ref, watch } from 'vue'

  const userlocation = ref('41.882629,-87.623474')
  const locationset = ref(false)
  const loading = ref(false)
  const items = ref([])
  const search = ref(null)
  const select = ref(null)
  const resultsobj = ref({})
  const tripinfo = ref('')
  //41.8786 Longitude: -87.6403.
  function setPosition(position) {
    console.log('position is ', position)
    userlocation.value = '' + position.coords.latitude + ',' + position.coords.longitude
    locationset.value = true
  }

  function setHello() {
    if (navigator.geolocation) {
      //const position =
      console.log('position')
      navigator.geolocation.getCurrentPosition(setPosition)
      console.log('done')
    } else {
      userlocation.value = 'no location available'
      console.log('no position')
    }
  }

  async function handleSelection(selectval, val) {
    // console.log('this is my select: ' + selectval + ' and val is ' + val)
    if (val == selectval) {
      console.log('Handling selection: ' + val)
      const selectobj = resultsobj.value[selectval]
      console.log('Obj: ' + JSON.stringify(selectobj))
      const lat = selectobj.lat
      const lon = selectobj.lon
      const url = `/api/routebylatlon?to=${lat},${lon}&from_=${userlocation.value}&arrival=2025-01-05T19:00:00Z`
      console.log('url: ' + url)
      const resp = await fetch(url)
      const results = await resp.json()
      tripinfo.value = JSON.stringify(results)
      console.log('results ' + JSON.stringify(results))
    }
  }

  setHello()

  watch(search, val => {
    val && val !== select.value && querySelections(val)
    handleSelection(select.value, val)
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
        resultsobj.value = results.results
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
        userlocation: 'Hello2 !'
      }
    },
  }
</script>
