<template>
  <AppBar />
  <v-main
    class="d-flex align-center justify-center"
    style="min-height: 300px"
  >
    <div>
      <button @click="goToSingleDirection">
        <b>Choose an origin</b>
      </button>
      <v-list @click:select="listSelected">
        <v-list-item
          v-for="item in landmarks.landmarks"
          :key="item.name"
          :value="item"
        >
          <v-list-item-title>{{ item.name }}</v-list-item-title>
        </v-list-item>
      </v-list>
    </div>
    <div>
      <table v-if="'response' in transitInfo">
        <tr v-if="'Northbound' in transitInfo.response">
          <td>Northbound</td>
          <td>{{ transitInfo.response.Northbound.length }}</td>
        </tr>
        <tr v-if="'Southbound' in transitInfo.response">
          <td>Southbound</td>
          <td>{{ transitInfo.response.Southbound.length }}</td>
        </tr>
        <tr v-if="'Eastbound' in transitInfo.response">
          <td>Eastbound</td>
          <td>{{ transitInfo.response.Eastbound.length }}</td>
        </tr>
        <tr v-if="'Westbound' in transitInfo.response">
          <td>Westbound</td>
          <td>{{ transitInfo.response.Westbound.length }}</td>
        </tr>
      </table>
    </div>
  </v-main>
  <AppFooter />
</template>

<script setup>
  import landmarks from "@/assets/landmarks.json"
  import { useAppStore } from '@/stores/app';
  import { onBeforeMount, ref } from 'vue'
  import { useRouter } from 'vue-router';

  import AppFooter from "./AppFooter.vue";
  import AppBar from "./AppBar.vue";

  const router = useRouter();
  const currentDirection = ref([]);
  const searchLocation = ref({lat: null, lon: null})
  const transitInfo = ref({})

  function goToSingleDirection() {
      router.push('/single-direction');
  }

  onBeforeMount(() => {
    console.log('mounting transit');
    console.log('current direction: ' + currentDirection.value);
    function setPosition(position) {
      searchLocation.value = {
        lat: position.coords.latitude,
        lon: position.coords.longitude
      }
      console.log('set location: ' + JSON.stringify(searchLocation.value))
    }

    if (navigator.geolocation) {
      console.log('setting geolocation')
      navigator.geolocation.getCurrentPosition(setPosition);
    }
  });

  // function errCallback(e) {
  //   console.log('error getting position: ' + e)
  // }

  function listSelected(event) {
    console.log('list item selected: ' + JSON.stringify(event.id));

    if (event.id.name === "My Current Location") {
      if (!searchLocation.value.lat) {
        console.log('No current location available');
        return;
      }
      console.log('Looking for current location');
    } else {
        searchLocation.value.lat = event.id.lat;
        searchLocation.value.lon = event.id.lon;
    }

    localStorage.clear();
    localStorage.setItem('searchLocation', JSON.stringify(searchLocation.value));
    const store = useAppStore();
    store.searchLocation = searchLocation;
    router.push('/single-direction');
  }
</script>
