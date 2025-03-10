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

const BACKEND_URL = `/api/single-estimate`;
  const estimateResponse = ref({})

  async function backendFetch(item) {
    const params = new URLSearchParams();
    params.set('pattern_id', item.pattern);
    params.set('stop_position', item.stop_position);
    params.set('vehicle_position', item.vehicle_position);
    const url = `${BACKEND_URL}?${params.toString()}`;
    console.log('fetching: ' + JSON.stringify(item) + " url " + url);
    //transitInfo.value = await (await fetch(url)).json();
    const response = await fetch(url);
    console.log('raw response: ' + JSON.stringify(response));
    estimateResponse.value =  await (response).json();
    //console.log('fetched ' + JSON.stringify(response));
  }


onMounted(() => {
      const store = useAppStore();
      console.log('displaying ' + JSON.stringify(store.currentDetail));
      currentDetail.value = store.currentDetail;
      console.log('current detail: ' + JSON.stringify(currentDetail.value));
      if (!Object.hasOwn(currentDetail.value, 'route')) {
        console.log('No current detail')
        currentDetail.value = JSON.parse(localStorage.getItem('currentDetail'))
      }
      backendFetch(currentDetail.value).then(() => {
        console.log('fetched estimate: ' + JSON.stringify(estimateResponse.value));
      }).catch(e => { console.log('fetch error: ' + e)});
    });

</script>
