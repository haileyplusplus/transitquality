<template>
  <v-app-bar>
    <div>Transit Single Direction</div>
  </v-app-bar>
  <v-main
    class="d-flex align-center justify-center"
    style="min-height: 300px"
  >
    <div v-if="currentDirection">
      Single direction
      <table
        v-for="item in currentDirection"
        :key="item.pattern"
      >
        <tr>
          <td rowspan="2">
            {{ item.route }}
          </td>
          <td rowspan="2">
            <img src="@/assets/woman.png">
          </td>
          <td>{{ item.distance_to_stop }}</td>
          <td rowspan="2">
            <img src="@/assets/front-of-bus.png">
          </td>
          <td>{{ item.distance_from_vehicle }} away</td>
        </tr>
        <tr>
          <td>{{ item.walk_time }}</td>
          <td>{{ item.displayed_estimate }}</td>
        </tr>
      </table>
    </div>
  </v-main>
</template>

<script>
  import { ref } from 'vue';
  import { useAppStore } from '@/stores/app';

const currentDirection = ref([]);

export default {
  mounted() {
      console.log('mounting single direction');
      const store = useAppStore();
      currentDirection.value = store.currentDirection;
      console.log('current direction (single): ' + currentDirection.value);
    }
}
</script>

<script setup>

// mounted() {
//   console.log('Mounted');
// }

</script>
