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
  Str: {{ hellostr }}
</template>

<script setup>
  import { ref, watch } from 'vue'

  const hellostr = ref('Hello!')

  function setPosition(position) {
    console.log('position is ', position)
    hellostr.value = '' + position.coords.latitude + ', ' + position.coords.longitude
  }

  function setHello() {
    if (navigator.geolocation) {
      //const position =
      console.log('position')
      navigator.geolocation.getCurrentPosition(setPosition)
      console.log('done')
    } else {
      hellostr.value = 'no location available'
      console.log('no position')
    }
  }

  function handleSelection(selectval) {
    console.log('this is my select: ' + selectval)
  }

  setHello()
  const loading = ref(false)
  const items = ref([])
  const search = ref(null)
  const select = ref(null)

  watch(search, val => {
    val && val !== select.value && querySelections(val)
    handleSelection(select.value)
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
        hellostr: 'Hello2 !'
      }
    },
  }
</script>
