<template>
  <h1>Choose Location</h1>
  <form>
    Latitude: <input
      v-model="selectedLat"
      type="text"
    ><br>
    Longitude: <input
      v-model="selectedLon"
      type="text"
    ><br>
    <v-btn @click="choose">
      Choose
    </v-btn>
  </form>
</template>

<script setup>
  import { ref } from 'vue';
  import { useRouter } from 'vue-router';
  import { useAppStore } from '@/stores/app';

  const router = useRouter();
  // Default location is Union Station
  const selectedLat = ref(41.878048);
  const selectedLon = ref(-87.640342)

  function choose() {
      const searchLocation = {'lat': selectedLat.value, 'lon': selectedLon.value}
      localStorage.clear();
      localStorage.setItem('searchLocation', JSON.stringify(searchLocation));
      const store = useAppStore();
      store.searchLocation = searchLocation;
      router.push('/single-direction');
  }
</script>
