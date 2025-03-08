<template>
  <v-main
    class="d-flex align-center justify-center"
    style="min-height: 300px"
  >
    <table
      v-if="currentDetail"
      style="max-width: 500px;"
    >
      <tbody>
        <tr>
          <td
            rowspan="3"
          >
            {{ currentDetail.route }}
          </td>
          <td colspan="2">
            {{ currentDetail.stop_name }}
          </td>
          <td rowspan="3">
                &nbsp;
          </td>
          <td colspan="2">
            {{ currentDetail.destination_stop_name }}
          </td>
        </tr>
        <tr>
          <td rowspan="2">
            <img
              src="@/assets/woman.png"
              height="40px"
            >
          </td>
          <td>{{ currentDetail.distance_to_stop }}</td>
          <td rowspan="2">
            <img
              v-if="currentDetail.mode == 'bus'"
              src="@/assets/front-of-bus.png"
              height="40px"
            >
            <img
              v-if="currentDetail.mode == 'train'"
              src="@/assets/underground.png"
              height="40px"
            >
          </td>
          <td>{{ currentDetail.distance_from_vehicle }} away</td>
        </tr>
        <tr>
          <td>{{ currentDetail.walk_time_minutes }} min</td>
          <td>{{ currentDetail.total_low_minutes }} - {{ currentDetail.total_high_minutes }} min</td>
        </tr>
      </tbody>
    </table>
  </v-main>
</template>


<script setup>
 //       currentDetail: {route: null, pattern: null, stopId: null, stopPosition: null, vehiclePosition: null}
  import { onMounted, ref } from 'vue';
  import { useAppStore } from '@/stores/app';

const currentDetail = ref(null);

const BACKEND_URL = `/api/estimates`;
  const estimateResponse = ref({})

  async function backendFetch(item) {
    const request = {
      estimates: [
        {
          pattern_id: item.pattern,
          stop_position: item.stop_position,
          vehicle_positions: [item.vehicle_position]
        }
      ]
    };
    console.log('fetching: ' + JSON.stringify(item) + " request (" + JSON.stringify(request) + ")");
    const url = `${BACKEND_URL}`;
    estimateResponse.value = await (await fetch(
      url, {
        method: "POST",
        mode: 'no-cors',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(request)
      }
    )).json();
  }


onMounted(() => {
      const store = useAppStore();
      console.log('displaying ' + JSON.stringify(store.currentDetail));
      currentDetail.value = store.currentDetail;
      backendFetch(currentDetail.value).then(() => {
        console.log('fetched estimate: ' + JSON.stringify(estimateResponse.value));
      });
    });

</script>
