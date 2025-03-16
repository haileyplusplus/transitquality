<template>
  <AppBar />
  <v-main
    class="d-flex align-center justify-center"
    style="min-height: 300px"
  >
    <div
      v-if="currentDirection"
    >
      <v-row>
        <v-col
          cols="12"
          class="ns"
          @click="chooseDirection('Northbound')"
        >
          <span
            class="innerns"
          >
            <v-card
              v-for="item in summaries.n"
              :key="item"
              class="buschip"
            >
              <v-card-item
                :style="routeColor(item)"
                @click.stop="chooseRoute(item, 'Northbound')"
              >
                <div class="text-h6 mb-1">
                  {{ routeDisp(item) }}
                </div>
              </v-card-item>
            </v-card>
          </span>
        </v-col>
      </v-row>
      <v-row>
        <v-col
          cols="5"
          class="ew"
          @click="chooseDirection('Eastbound')"
        >
          <span class="innerew">
            <v-card
              v-for="item in summaries.w"
              :key="item"
              class="buschip"
            >
              <v-card-item
                :style="routeColor(item)"
                @click.stop="chooseRoute(item, 'Westbound')"
              >
                <div class="text-h6 mb-1">
                  {{ routeDisp(item) }}
                </div>
              </v-card-item>
            </v-card>
          </span>
        </v-col>
        <v-col
          cols="2"
          class="compass"
          @click="chooseDirection('all')"
        >
          <img
            src="@/assets/compass.png"
            height="40px"
          >
        </v-col>
        <v-col
          cols="5"
          class="ew"
          @click="chooseDirection('Westbound')"
        >
          <span class="innerew">
            <v-card
              v-for="item in summaries.e"
              :key="item"
              class="buschip"
            >
              <v-card-item
                :style="routeColor(item)"
                @click.stop="chooseRoute(item, 'Eastbound')"
              >
                <div class="text-h6 mb-1">
                  {{ routeDisp(item) }}
                </div>
              </v-card-item>
            </v-card>
          </span>
        </v-col>
      </v-row>
      <v-row>
        <v-col
          cols="12"
          class="ns"
          @click="chooseDirection('Southbound')"
        >
          <span class="innerns">
            <v-card
              v-for="item in summaries.s"
              :key="item"
              class="buschip"
            >
              <v-card-item
                :style="routeColor(item)"
                @click.stop="chooseRoute(item, 'Southbound')"
              >
                <div class="text-h6 mb-1">
                  {{ routeDisp(item) }}
                </div>
              </v-card-item>
            </v-card>
          </span>
        </v-col>
      </v-row>

      <table style="max-width: 500px;">
        <tbody>
          <tr style="height: 20px;">
            <td />
          </tr>
          <tr v-if="currentDirection.length == 0">
            <td><b>{{ getFilterInfo() }}</b></td>
          </tr>
          <template
            v-for="item in currentDirection"
            :key="item.pattern + '-' + item.vehicle"
          >
            <tr @click="itemDetail(item)">
              <td
                rowspan="3"
                :style="routeColor(item.route)"
              >
                {{ routeDisp(item.route) }}
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
            <tr @click="itemDetail(item)">
              <td rowspan="2">
                <img
                  src="@/assets/woman.png"
                  height="40px"
                >
              </td>
              <td>{{ item.distance_to_stop }}</td>
              <td rowspan="2">
                <img
                  v-if="item.mode == 'bus'"
                  src="@/assets/front-of-bus.png"
                  height="40px"
                >
                <img
                  v-if="item.mode == 'train'"
                  src="@/assets/underground.png"
                  height="40px"
                >
              </td>
              <td>{{ item.distance_from_vehicle }} away</td>
            </tr>
            <tr @click="itemDetail(item)">
              <td>{{ item.walk_time_minutes }} min</td>
              <td>{{ item.total_low_minutes }} - {{ item.total_high_minutes }} min</td>
            </tr>
          </template>
        </tbody>
      </table>
    </div>
  </v-main>
  <AppFooter />
</template>

<script setup>

// mounted() {
//   console.log('Mounted');
// }
import { onMounted, ref } from 'vue';
  import { useAppStore } from '@/stores/app';
  import { useRouter } from 'vue-router';

import AppFooter from './AppFooter.vue';
import AppBar from './AppBar.vue';

  const router = useRouter();
  import routeColors from '@/assets/route-colors.json'

const currentDirection = ref(null);
const summaries = ref(null);

const colorMap = new Map();

      {/* return {
        colorMap: colorMap
      } */}

      function routeColor(route) {
        //console.log('getting route ' + JSON.stringify(route));
        var obj = this.colorMap.get(route.toLowerCase())
        //console.log('getting color for ' + route.toLowerCase() + ': ' + JSON.stringify(obj));
        if (!obj) {
          return { backgroundColor: 'none' }
        }
        return obj
      }

    function chooseDirection(dir) {
      console.log('chose direction: ' + dir);
      const store = useAppStore();
      if (dir == 'all') {
        store.directionFilter = null;
        currentDirection.value = store.currentDirection;
        return;
      }
      currentDirection.value = store.currentDirection.filter((item) => {
        return item.direction == dir;
      });
      store.directionFilter = dir;
      store.routeFilter = null;
      localStorage.setItem('filters', JSON.stringify({
        directionFilter: store.directionFilter,
        routeFilter: store.routeFilter
      }));
    }

    function chooseRoute(rt, dir) {
      console.log('chose route: ' + JSON.stringify(rt));
      const store = useAppStore();
      currentDirection.value = store.currentDirection.filter((item) => {
        return item.route == rt && item.direction == dir;
      });
      store.routeFilter = rt;
      store.directionFilter = dir;
      localStorage.setItem('filters', JSON.stringify({
        directionFilter: store.directionFilter,
        routeFilter: store.routeFilter
      }));
    }

    function routeDisp(route) {
      const routes = {
        brn: 'Br',
        blue: 'Bl',
        p: 'Pu',
        pink: 'Pk',
        g: 'G',
        red: 'R',
        y: 'Y',
        org: 'O'
      };
      const rv = routes[route];
      //console.log('route ' + route + ' is ' + rv);
      if (!rv) {
        return route;
      }
      return rv;
    }


    function itemDetail(item) {
      console.log('selected for detail 2: ' + JSON.stringify(item));
      const store = useAppStore();
      store.currentDetail = item;
      localStorage.setItem('currentDetail', JSON.stringify(store.currentDetail));
      router.push('/detail');
    }

    function getFilterInfo() {
      const filters = JSON.parse(localStorage.getItem('filters', '{}'));
      if (filters) {
        console.log('store df ' + filters.directionFilter);
        if (filters.directionFilter) {
          if (filters.routeFilter) {
            return 'No information currently available for ' + filters.directionFilter + ' ' + filters.routeFilter;
          }
          return 'No information currently available for ' + filters.directionFilter + ' routes';
        }
        return 'No information currently available for these routes';
       }
    }

onMounted(() => {
  routeColors.colors.forEach((elem) => {
        colorMap.set(elem.route.toLowerCase(), elem);
        //delete elem.route;
      });

  console.log('mounting single direction: ' + new Date());
      const store = useAppStore();
      currentDirection.value = store.currentDirection;
      summaries.value = store.summaries;
      if (!currentDirection.value && (localStorage.getItem('currentDirection') !== null)) {
        currentDirection.value = JSON.parse(localStorage.getItem('currentDirection'));
        summaries.value = JSON.parse(localStorage.getItem('summaries'));
        store.currentDirection = currentDirection.value;
        store.summaries = summaries.value;
      }
      // summaries.value.n.forEach((elem, index, arr) => {
      //   arr[index] = this.routeDisp(elem);
      // });
      console.log('current direction (single): ' + currentDirection.value);
      console.log('displaying ' + JSON.stringify(store.summaries));

      const filters = JSON.parse(localStorage.getItem('filters', '{}'));
      if (filters) {
        console.log('store df ' + filters.directionFilter);
        if (filters.directionFilter) {
          if (filters.routeFilter) {
            chooseRoute(filters.routeFilter, filters.directionFilter);
          } else {
            chooseDirection(filters.directionFilter);
          }
        }
      }
})

</script>

<style scoped>
.buschip {
  min-width: 70px;
  max-width: 70px;
  text-align: center;
}
.ns {
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #eeeeee;
}
.ew {
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #eeee00;
}
.innerns {
  display: grid;
  margin: auto;
  align-items: center;
  grid-template-columns: 1fr 1fr 1fr 1fr;
  grid-gap: 10px;
}
.innerew {
  display: grid;
  margin: auto;
  grid-template-columns: 1fr 1fr;
  grid-gap: 10px;
}
.compass {
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: pink;
}
</style>
