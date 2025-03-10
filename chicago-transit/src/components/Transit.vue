<template>
  <v-app-bar>
    <div>Transit</div>
  </v-app-bar>
  <v-main
    class="d-flex align-center justify-center"
    style="min-height: 300px"
  >
    <div>
      <button @click="goToSingleDirection">
        Single direction
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
</template>

<script>
  import landmarks from "@/assets/landmarks.json"
  import { useAppStore } from '@/stores/app';

  export const currentDirection = ref([]);
  const searchLocation = ref({lat: null, lon: null})

  export default {
    data() {
      return {
        landmarks: landmarks
      }
    },
    beforeMount() {
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
    },
    methods: {
      goToSingleDirection() {
        this.$router.push('/single-direction');
      }
    }
  }
</script>

<script setup>
  import { ref } from 'vue'
  import { useRouter } from 'vue-router';
  //import { useGeolocation } from '@vueuse/core'

  const router = useRouter();

  const BACKEND_URL = `/api/combined-estimate`;
  const transitInfo = ref({})

  async function backendFetch(lat, lon) {
    const url = `${BACKEND_URL}?lat=${lat}&lon=${lon}`;
    transitInfo.value = await (await fetch(url)).json();
  }

  function getRoutes(items) {
    var rv = new Set([]);
    items.forEach((elem) => {
      rv.add(elem.route);
    });
    return Array.from(rv);
  }


  // function errCallback(e) {
  //   console.log('error getting position: ' + e)
  // }

  function listSelected(event) {
    console.log('list item selected: ' + JSON.stringify(event.id));

    //var getSearchLocation = null;

    if (event.id.name === "My Current Location") {
      if (!searchLocation.value.lat) {
        console.log('No current location available');
        return;
      }
      console.log('Looking for current location');
      // const { coords, locatedAt, error, resume, pause } = useGeolocation()
      // if (error) {
      //   console.log('Geolocation error: ' + error);
      //   return;
      // }
      // lat = coords.latitude;
      // lon = coords.longitude;
      // if (navigator.geolocation) {
      //   console.log('setting geolocation')
      //   //getSearchLocation = navigator.geolocation.getCurrentPosition(setPosition, errCallback);
      //   getSearchLocation = () => {
      //     return new Promise(function (resolve, reject) {
      //       navigator.geolocation.getCurrentPosition((position) => resolve(position),
      //       (err) => reject(err));
      //     });
      //   }
      //   console.log('setting to ' + getSearchLocation)
      // } else {
      //   console.log('location not available');
      // }
    } else {
        searchLocation.value.lat = event.id.lat;
        searchLocation.value.lon = event.id.lon;
    }

    // console.log('search function: ' + getSearchLocation);

    // if (!getSearchLocation) {
    //   console.log("no position");
    //   return;
    // }

    backendFetch(searchLocation.value.lat, searchLocation.value.lon).then(
        () => {
          console.log('fetched');
          if ('response' in transitInfo.value) {
            const store = useAppStore();
            var dirs = [];
            if ('Northbound' in transitInfo.value.response) {
              store.currentDirection = transitInfo.value.response.Northbound;
              dirs.push(...transitInfo.value.response.Northbound);
              store.summaries.n = getRoutes(transitInfo.value.response.Northbound);
            }
            if ('Westbound' in transitInfo.value.response) {
              dirs.push(...transitInfo.value.response.Westbound);
              store.summaries.w = getRoutes(transitInfo.value.response.Westbound);
            }
            if ('Eastbound' in transitInfo.value.response) {
              dirs.push(...transitInfo.value.response.Eastbound);
              store.summaries.e = getRoutes(transitInfo.value.response.Eastbound);
            }
            if ('Southbound' in transitInfo.value.response) {
              dirs.push(...transitInfo.value.response.Southbound);
              store.summaries.s = getRoutes(transitInfo.value.response.Southbound)
            }
            store.currentDirection = dirs.filter((elem) => elem.display);
            localStorage.setItem('summaries', JSON.stringify(store.summaries));
            localStorage.setItem('currentDirection', JSON.stringify(store.curretnDirection));
          }
          //console.log('current direction: ' + JSON.stringify(currentDirection.value));
          //this.$router.push('/single-direction')
          router.push('/single-direction');
        }
      );
  }
</script>
