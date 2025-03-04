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

  export default {
    data() {
      return {
        landmarks: landmarks
      }
    },
    beforeMount() {
      console.log('mounting transit');
      console.log('current direction: ' + currentDirection.value);
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

  const BACKEND_URL = `/api/combined-estimate`;
  const transitInfo = ref({})

  async function backendFetch(lat, lon) {
    const url = `${BACKEND_URL}?lat=${lat}&lon=${lon}`;
    transitInfo.value = await (await fetch(url)).json();
  }

  function listSelected(event) {
    console.log('list item selected: ' + JSON.stringify(event.id.lon));
    backendFetch(event.id.lat, event.id.lon).then(
      () => {
        console.log('fetched');
        if ('response' in transitInfo.value && 'Northbound' in transitInfo.value.response) {
          currentDirection.value = transitInfo.value.response.Northbound;
          const store = useAppStore();
          store.currentDirection = transitInfo.value.response.Northbound;
        }
        //console.log('current direction: ' + JSON.stringify(currentDirection.value));
        //this.$router.push('/single-direction')
      }
    );
  }
</script>
