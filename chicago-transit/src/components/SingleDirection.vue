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
      <table>
        <tbody>
          <template
            v-for="item in currentDirection"
            :key="item.pattern"
          >
            <tr>
              <td
                rowspan="3"
                :style="routeColor(item.route)"
              >
                {{ item.route }}
              </td>
              <td colspan="2">
                {{ item.stop_name }}
              </td>
              <td rowspan="3">
                &nbsp;
              </td>
              <td colspan="2">
                {{ item.destination_stop_name }}
              </td>
            </tr>
            <tr>
              <td rowspan="2">
                <img
                  src="@/assets/woman.png"
                  height="40px"
                >
              </td>
              <td>{{ item.distance_to_stop }}</td>
              <td rowspan="2">
                <img
                  src="@/assets/front-of-bus.png"
                  height="40px"
                >
              </td>
              <td>{{ item.distance_from_vehicle }} away</td>
            </tr>
            <tr>
              <td>{{ item.walk_time_minutes }} min</td>
              <td>{{ item.total_low_minutes }} - {{ item.total_high_minutes }} min</td>
            </tr>
          </template>
        </tbody>
      </table>
    </div>
  </v-main>
</template>

<script>
  import { ref } from 'vue';
  import { useAppStore } from '@/stores/app';
  import routeColors from '@/assets/route-colors.json'

const currentDirection = ref([]);

export default {
    data() {
      const colorMap = new Map();
      routeColors.colors.forEach((elem) => {
        colorMap.set(elem.route, elem);
      });
      return {
        colorMap: colorMap
      }
    },
    mounted() {
      console.log('mounting single direction');
      const store = useAppStore();
      currentDirection.value = store.currentDirection;
      console.log('current direction (single): ' + currentDirection.value);

      const testData = [{
        "pattern": 6414,
        "vehicle": 4335,
        "route": "151",
        "mode": "bus",
        "direction": "Northbound",
        "stop_id": 1168,
        "stop_name": "Sheridan & Byron",
        "stop_lat": 41.952829999999,
        "stop_lon": -87.654805000001,
        "stop_position": "6.88 mi",
        "vehicle_position": "0.00 mi",
        "distance_from_vehicle": "6.88 mi",
        "distance_to_stop": "0.40 mi",
        "last_update": "2025-03-04T22:27:44",
        "age_seconds": 51,
        "destination_stop_id": 14176,
        "destination_stop_name": "Clark & Arthur Terminal",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "low_estimate_minutes": 1,
        "high_estimate_minutes": 5,
        "walk_time_minutes": 10,
        "total_low_minutes": 0,
        "total_high_minutes": 4,
        "walk_distance": "0.50 mi",
        "display": false
      },
      {
        "pattern": 6414,
        "vehicle": 4388,
        "route": "151",
        "mode": "bus",
        "direction": "Northbound",
        "stop_id": 1168,
        "stop_name": "Sheridan & Byron",
        "stop_lat": 41.952829999999,
        "stop_lon": -87.654805000001,
        "stop_position": "6.88 mi",
        "vehicle_position": "6.35 mi",
        "distance_from_vehicle": "0.54 mi",
        "distance_to_stop": "0.40 mi",
        "last_update": "2025-03-04T22:27:43",
        "age_seconds": 52,
        "destination_stop_id": 14176,
        "destination_stop_name": "Clark & Arthur Terminal",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "low_estimate_minutes": 2,
        "high_estimate_minutes": 3,
        "walk_time_minutes": 10,
        "total_low_minutes": 1,
        "total_high_minutes": 2,
        "walk_distance": "0.50 mi",
        "display": false
      },
      {
        "pattern": 6414,
        "vehicle": 4123,
        "route": "151",
        "mode": "bus",
        "direction": "Northbound",
        "stop_id": 1168,
        "stop_name": "Sheridan & Byron",
        "stop_lat": 41.952829999999,
        "stop_lon": -87.654805000001,
        "stop_position": "6.88 mi",
        "vehicle_position": "2.64 mi",
        "distance_from_vehicle": "4.25 mi",
        "distance_to_stop": "0.40 mi",
        "last_update": "2025-03-04T22:27:56",
        "age_seconds": 39,
        "destination_stop_id": 14176,
        "destination_stop_name": "Clark & Arthur Terminal",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "low_estimate_minutes": 16,
        "high_estimate_minutes": 29,
        "walk_time_minutes": 10,
        "total_low_minutes": 16,
        "total_high_minutes": 29,
        "walk_distance": "0.50 mi",
        "display": true
      },
      {
        "pattern": 6414,
        "vehicle": 4392,
        "route": "151",
        "mode": "bus",
        "direction": "Northbound",
        "stop_id": 1168,
        "stop_name": "Sheridan & Byron",
        "stop_lat": 41.952829999999,
        "stop_lon": -87.654805000001,
        "stop_position": "6.88 mi",
        "vehicle_position": "1.65 mi",
        "distance_from_vehicle": "5.23 mi",
        "distance_to_stop": "0.40 mi",
        "last_update": "2025-03-04T22:27:43",
        "age_seconds": 52,
        "destination_stop_id": 14176,
        "destination_stop_name": "Clark & Arthur Terminal",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "low_estimate_minutes": 24,
        "high_estimate_minutes": 42,
        "walk_time_minutes": 10,
        "total_low_minutes": 23,
        "total_high_minutes": 41,
        "walk_distance": "0.50 mi",
        "display": true
      },
      {
        "pattern": 6414,
        "vehicle": 4199,
        "route": "151",
        "mode": "bus",
        "direction": "Northbound",
        "stop_id": 1168,
        "stop_name": "Sheridan & Byron",
        "stop_lat": 41.952829999999,
        "stop_lon": -87.654805000001,
        "stop_position": "6.88 mi",
        "vehicle_position": "0.00 mi",
        "distance_from_vehicle": "6.88 mi",
        "distance_to_stop": "0.40 mi",
        "last_update": "2025-03-04T22:14:00",
        "age_seconds": 24,
        "destination_stop_id": 14176,
        "destination_stop_name": "Clark & Arthur Terminal",
        "waiting_to_depart": true,
        "predicted_minutes": 5,
        "low_estimate_minutes": 47,
        "high_estimate_minutes": 62,
        "walk_time_minutes": 10,
        "total_low_minutes": 52,
        "total_high_minutes": 67,
        "walk_distance": "0.50 mi",
        "display": false
      },
      {
        "pattern": 308500008,
        "vehicle": 826,
        "route": "red",
        "mode": "train",
        "direction": "Northbound",
        "stop_id": 30273,
        "stop_name": "Addison-Red",
        "stop_lat": 41.947428,
        "stop_lon": -87.653626,
        "stop_position": "16.55 mi",
        "vehicle_position": "8.38 mi",
        "distance_from_vehicle": "8.18 mi",
        "distance_to_stop": "0.15 mi",
        "last_update": "2025-03-04T22:28:01",
        "age_seconds": 34,
        "destination_stop_id": 30173,
        "destination_stop_name": "Howard",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "low_estimate_minutes": 26,
        "high_estimate_minutes": 34,
        "walk_time_minutes": 3,
        "total_low_minutes": 26,
        "total_high_minutes": 33,
        "walk_distance": "0.15 mi",
        "display": false
      }];

      if (!currentDirection.value) {
        console.log('using test data')
        currentDirection.value = testData;
      }

    },
    methods: {
      routeColor: function(route) {
        var obj = this.colorMap.get(route)
        // console.log('getting color for ' + route + ': ' + JSON.stringify(obj));
        if (!obj) {
          return { backgroundColor: 'none' }
        }
        return {
          backgroundColor: obj.bg,
          color: obj.fg
        }
      }
    }
}

</script>

<script setup>

// mounted() {
//   console.log('Mounted');
// }

</script>
