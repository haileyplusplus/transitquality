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
        <tr v-if="estimateResponse">
          <td colspan="6">
            <v-card>
              <v-card-item>
                <div class="text-h6 mb-1">
                  Vehicle id: {{ currentDetail.vehicle }}
                </div>
              </v-card-item>
              <v-card-item>
                <div class="text-h6 mb-1">
                  Last update: {{ currentDetail.timestamp }}
                </div>
              </v-card-item>
              <v-card-item>
                <div class="text-h6 mb-1">
                  Recent arrivals at this stop
                </div>
              </v-card-item>
              <v-card-item>
                <apexchart
                  width="500"
                  type="bar"
                  :options="chartOptions"
                  :series="chartSeries"
                />
              </v-card-item>
            </v-card>
          </td>
        </tr>
      </tbody>
    </table>
  </v-main>
</template>


<script setup>
 //       currentDetail: {route: null, pattern: null, stopId: null, stopPosition: null, vehiclePosition: null}
  import { onMounted, ref } from 'vue';
  import { useAppStore } from '@/stores/app';
  //import VueApexCharts from 'vue-apexcharts'

const currentDetail = ref(null);
const estimateResponse = ref(null);
const chartOptions = ref({
  chart: {id: 'arrivalDetail'},
  plotOptions: {bar: {horizontal: true}}
});

const chartSeries = ref([])

const BACKEND_URL = `/api/single-estimate`;

  async function backendFetch(item) {
    const params = new URLSearchParams();
    params.set('pattern_id', item.pattern);
    params.set('stop_position', item.stop_position);
    params.set('vehicle_position', item.vehicle_position);
    params.set('vehicle_id', item.vehicle);
    const url = `${BACKEND_URL}?${params.toString()}`;
    console.log('fetching: ' + JSON.stringify(item) + " url " + url);
    //transitInfo.value = await (await fetch(url)).json();
    const response = await fetch(url);
    //console.log('raw response: ' + JSON.stringify(response));
    const jd =  await (response).json();
    //const estimates = jd.patterns[0].single_estimates[0].info.estimates;
    const outer = jd.patterns[0].single_estimates[0];
    currentDetail.value.distance_from_vehicle = outer.distance_to_vehicle_mi;
    currentDetail.value.total_low_minutes = outer.low_mins;
    currentDetail.value.total_high_minutes = outer.high_mins;
    currentDetail.value.timestamp = new Date(outer.timestamp).toLocaleString();
    currentDetail.value.vehicle = outer.vehicle_id;
    const estimates = outer.info.estimates;
    console.log('estimates raw ' + JSON.stringify(outer));
    estimateResponse.value = estimates;
    //const estimate = estimates.patterns[0].single_estimates.info;
    //console.log('estimate: ' + estimate)
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
        const data = new Array();
        chartOptions.value.xaxis = {};
        chartOptions.value.dataLabels = {
          enabled: true,
          formatter: function (val) {
            return val + ' min';
          }
        };

        chartOptions.value.xaxis.categories = new Array();
        estimateResponse.value.forEach((item) => {
          const date = new Date(item.timestamp);
          //data.push();
          data.push('' + Math.round(item.travel_time) + ' minutes');
          chartOptions.value.xaxis.categories.push(date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit"}));
        });
        chartSeries.value = [{name: "arrival-times", data: data}]
        console.log('chart options: ' + JSON.stringify(chartOptions.value));
      }).catch(e => { console.log('fetch error: ' + e)});

    });

</script>
