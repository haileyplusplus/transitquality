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
              <td rowspan="3">
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

const currentDirection = ref([]);

export default {
  mounted() {
      console.log('mounting single direction');
      const store = useAppStore();
      currentDirection.value = store.currentDirection;
      console.log('current direction (single): ' + currentDirection.value);

      const testData = [{
        "query_start": "2025-03-04T15:34:13.939605",
        "pattern": 4288,
        "route": "15",
        "direction": "Northbound",
        "stop_id": 5307,
        "stop_name": "Jeffery & 71st Street",
        "stop_lat": 41.766584000001,
        "stop_lon": -87.576223,
        "stop_position": "8696.55 meter",
        "vehicle_position": "7096.35 meter",
        "distance_from_vehicle": "0.99 mile",
        "distance_to_stop": "815.14 meter",
        "last_update": "2025-03-03T22:31:12",
        "age": "PT17H3M2S",
        "destination_stop_id": 14784,
        "destination_stop_name": "47th Red Line Station",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "displayed_estimate": "3-5 min",
        "low_estimate": "-PT16H59M3.606235S",
        "high_estimate": "-PT16H57M8.716966S",
        "walk_time": "9 minutes",
        "walk_distance": "0.52 mile",
        "display": true,
        "trace_info": {
          "2025-03-03T19:23:05": {
            "redis_key": "busposition:4288:261852649",
            "from": 7318.5527999999995,
            "to": 8928.506399999998,
            "travel_time": 4.3,
            "travel_dist": 1609.953599999999,
            "travel_rate": 6.288881249999996,
            "raw_estimate": 4.2
          },
          "2025-03-03T19:17:58": {
            "redis_key": "busposition:4288:261852654",
            "from": 7212.177599999999,
            "to": 8706.916799999999,
            "travel_time": 5.5,
            "travel_dist": 1494.7392,
            "travel_rate": 4.529512727272727,
            "raw_estimate": 5.9
          },
          "2025-03-03T19:53:53": {
            "redis_key": "busposition:4288:261852655",
            "from": 7375.245599999999,
            "to": 8835.237599999999,
            "travel_time": 4.7,
            "travel_dist": 1459.9919999999993,
            "travel_rate": 5.232946236559137,
            "raw_estimate": 5.1
          },
          "2025-03-03T20:10:43": {
            "redis_key": "busposition:4288:261852675",
            "from": 7374.940799999999,
            "to": 8717.279999999999,
            "travel_time": 4.2,
            "travel_dist": 1342.3391999999994,
            "travel_rate": 5.3479649402390415,
            "raw_estimate": 5
          },
          "2025-03-03T20:31:27": {
            "redis_key": "busposition:4288:261852659",
            "from": 7375.245599999999,
            "to": 8896.1976,
            "travel_time": 5.5,
            "travel_dist": 1520.9520000000002,
            "travel_rate": 4.637048780487806,
            "raw_estimate": 5.8
          },
          "2025-03-03T20:46:29": {
            "redis_key": "busposition:4288:261852698",
            "from": 7142.987999999999,
            "to": 8700.516,
            "travel_time": 5.3,
            "travel_dist": 1557.5280000000002,
            "travel_rate": 4.882532915360502,
            "raw_estimate": 5.5
          },
          "2025-03-03T21:08:06": {
            "redis_key": "busposition:4288:261852662",
            "from": 7375.245599999999,
            "to": 9037.9296,
            "travel_time": 4.3,
            "travel_dist": 1662.6840000000002,
            "travel_rate": 6.469587548638133,
            "raw_estimate": 4.1
          },
          "2025-03-03T21:28:39": {
            "redis_key": "busposition:4288:261852660",
            "from": 7097.267999999999,
            "to": 8896.1976,
            "travel_time": 4.5,
            "travel_dist": 1798.9296000000004,
            "travel_rate": 6.712423880597016,
            "raw_estimate": 4
          },
          "2025-03-03T21:53:30": {
            "redis_key": "busposition:4288:261852671",
            "from": 7211.567999999999,
            "to": 8894.673599999998,
            "travel_time": 5.2,
            "travel_dist": 1683.105599999999,
            "travel_rate": 5.343192380952377,
            "raw_estimate": 5
          },
          "2025-03-03T22:10:31": {
            "redis_key": "busposition:4288:261852674",
            "from": 7375.245599999999,
            "to": 9240.926399999998,
            "travel_time": 3.1,
            "travel_dist": 1865.6807999999992,
            "travel_rate": 9.923834042553187,
            "raw_estimate": 2.7
          },
          "stdev": 58.45248567875426,
          "considered": [
            254.44907232109063,
            353.28303425774874,
            305.793319415449,
            299.2166212534061,
            345.0901803607213,
            327.7397260273972,
            247.34188817598528,
            238.39376482548283,
            299.4838826512135
          ],
          "bus_position": 7096.3535999999995
        }
      },
      {
        "query_start": "2025-03-04T15:34:13.939605",
        "pattern": 7032,
        "route": "71",
        "direction": "Northbound",
        "stop_id": 10819,
        "stop_name": "71st Street & Yates",
        "stop_lat": 41.766308,
        "stop_lon": -87.566877000001,
        "stop_position": "10214.46 meter",
        "vehicle_position": "5365.09 meter",
        "distance_from_vehicle": "3.01 mile",
        "distance_to_stop": "38.36 meter",
        "last_update": "2025-03-03T22:31:13",
        "age": "PT17H3M1S",
        "destination_stop_id": 14140,
        "destination_stop_name": "69th Red Line Station",
        "waiting_to_depart": false,
        "predicted_minutes": null,
        "displayed_estimate": "2-6 min",
        "low_estimate": "-PT16H51M36.472296S",
        "high_estimate": "-PT16H44M51.134443S",
        "walk_time": "1 minute",
        "walk_distance": "0.02 mile",
        "display": true,
        "trace_info": {
          "2025-03-03T19:47:17": {
            "redis_key": "busposition:7032:259624301",
            "from": 5456.224799999999,
            "to": 10536.631199999998,
            "travel_time": 16.7,
            "travel_dist": 5080.406399999999,
            "travel_rate": 5.070265868263472,
            "raw_estimate": 15.9
          },
          "2025-03-03T20:12:41": {
            "redis_key": "busposition:7032:259624303",
            "from": 5444.032799999999,
            "to": 10735.970399999998,
            "travel_time": 12.4,
            "travel_dist": 5291.937599999999,
            "travel_rate": 7.084253815261043,
            "raw_estimate": 11.4
          },
          "2025-03-03T19:59:23": {
            "redis_key": "busposition:7032:259624302",
            "from": 5459.5776,
            "to": 10220.5536,
            "travel_time": 17.8,
            "travel_dist": 4760.976,
            "travel_rate": 4.449510280373832,
            "raw_estimate": 18.2
          },
          "2025-03-03T20:30:34": {
            "redis_key": "busposition:7032:259624304",
            "from": 5395.264799999999,
            "to": 10603.687199999998,
            "travel_time": 19.2,
            "travel_dist": 5208.4223999999995,
            "travel_rate": 4.509456623376623,
            "raw_estimate": 17.9
          },
          "2025-03-03T20:43:05": {
            "redis_key": "busposition:7032:259624305",
            "from": 5670.499199999999,
            "to": 10535.716799999998,
            "travel_time": 15.4,
            "travel_dist": 4865.217599999999,
            "travel_rate": 5.265387012987012,
            "raw_estimate": 15.3
          },
          "2025-03-03T20:58:40": {
            "redis_key": "busposition:7032:259624306",
            "from": 5441.594399999999,
            "to": 10214.457599999998,
            "travel_time": 14.2,
            "travel_dist": 4772.863199999999,
            "travel_rate": 5.588832786885245,
            "raw_estimate": 14.5
          },
          "2025-03-03T21:17:21": {
            "redis_key": "busposition:7032:259624307",
            "from": 5444.032799999999,
            "to": 10219.639199999998,
            "travel_time": 13.4,
            "travel_dist": 4775.606399999999,
            "travel_rate": 5.954621446384039,
            "raw_estimate": 13.6
          },
          "2025-03-03T21:38:31": {
            "redis_key": "busposition:7032:259624308",
            "from": 5444.032799999999,
            "to": 10589.666399999998,
            "travel_time": 12.6,
            "travel_dist": 5145.633599999999,
            "travel_rate": 6.797402377807132,
            "raw_estimate": 11.9
          },
          "2025-03-03T21:48:51": {
            "redis_key": "busposition:7032:259624309",
            "from": 5395.569599999999,
            "to": 10536.021599999998,
            "travel_time": 13.7,
            "travel_dist": 5140.451999999999,
            "travel_rate": 6.245992709599027,
            "raw_estimate": 12.9
          },
          "2025-03-03T22:13:50": {
            "redis_key": "busposition:7032:259624310",
            "from": 5463.235199999999,
            "to": 10457.078399999999,
            "travel_time": 17.4,
            "travel_dist": 4993.843199999999,
            "travel_rate": 4.797159654178674,
            "raw_estimate": 16.8
          },
          "stdev": 144.07154175343322,
          "considered": [
            956.4326853851692,
            684.5277041815458,
            1089.8655569782327,
            1075.3774578651683,
            920.9898508958776,
            867.6888690210102,
            814.3872861884095,
            713.4148797535836,
            776.3966795137859,
            1010.8831787109374
          ],
          "bus_position": 5365.089599999999
        }
      }];

      if (!currentDirection.value) {
        console.log('using test data')
        currentDirection.value = testData;
      }

    }
}
</script>

<script setup>

// mounted() {
//   console.log('Mounted');
// }

</script>
