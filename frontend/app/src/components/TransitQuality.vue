<script>
export default {
  name: 'TransitQuality',
  props: {
    msg: String
  },
  data() {
    return {
      tripId: 74250,
      singleTripData: {"trips": []},
      routeData: {"routes": []},
      stopId: null,
      routeId: '1',
      day: '2024-12-20',
      stopData: {"stops": []},
      dayData: {"days": []},
      tripData: {"trips": []}
    }
  },
  methods: {
    async fetchRoutes() {
      this.routeData = {"routes": []}
      const res = await fetch(
        `http://localhost:8085/api/routes`
      )
      this.routeData = await res.json()
      const res2 = await fetch(
        `http://localhost:8085/api/days`
      )
      this.dayData = await res2.json()      
    },
    async fetchData() {
      this.singleTripData = {"trips": []}
      const res = await fetch(
        `http://localhost:8085/api/trip/${this.tripId}`
      )
      this.singleTripData = await res.json()
    },
    async fetchTrip() {
      this.tripData = {"trips": []}
      const res = await fetch(
        `http://localhost:8085/api/trips/${this.routeId}?day=${this.day}`
      )
      this.tripData = await res.json()
    },
    fetchSingleTrip() {
      this.fetchData()
      this.tripId = ''
    },
    async fetchStop() {
      this.stopData = {"stops": []}
      const res = await fetch(
        `http://localhost:8085/api/stops/${this.stopId}?route_id=${this.routeId}&day=${this.day}`
      )
      this.stopData = await res.json()
    },
    selectStop(stop_id, route_id, day) {
      this.stopId = stop_id
      this.routeId = route_id
      //this.day = day
      this.fetchStop()
    },
    selectRoute(route_id) {
      console.log(`Selected route ${route_id}`)
      this.routeId = route_id
    },
    selectDay(day) {
      console.log(`Selected day ${day}`)
      this.day = day
      this.fetchTrip()
    },
    selectTrip(trip_id) {
      console.log(`Selected trip ${trip_id}`)
      this.tripId = trip_id
      this.fetchData()
    }
},
  mounted() {
    this.fetchRoutes()    
  }
}
</script>

<template>
  <div class="trips">
    <h1>Routes</h1>
    <form @submit.prevent="">      
      <div v-if="routeData['routes']">
        <table>
          <tr v-for="(route, index) in routeData['routes']" :key="route.route_id">
            <td>
              <button @click="selectRoute(route.route_id)">{{ route.route_id }} - {{ route.name }}</button>
            </td>
          </tr>        
      </table>
      </div>
    </form>

    <h1>Days</h1>
    <form @submit.prevent="">      
      <div v-if="dayData['days']">
        <table>
          <tr v-for="(day, index) in dayData['days']" :key="day">
            <td>
              <button @click="selectDay(day)">{{ day }}</button>
            </td>
          </tr>        
      </table>
      </div>
    </form>

    <h1>Trips</h1>
    <div v-if="tripData['trips'].length > 0">
    <form @submit.prevent="">
    <table>      
      <tr v-for="trip in tripData['trips']" :key="trip.trip_id">
        <td>{{ trip.origtatripno }}</td>
        <td>{{ new Date(trip.schedule_time).toLocaleString() }}</td>
        <td v-if="trip.route">{{ trip.route.route_id }}</td>
        <td v-if="trip.pattern.direction">{{ trip.pattern.direction.direction_id }}</td>
        <td v-if="trip.pattern">{{ (trip.pattern.length / 5280.0).toFixed(2) }}mi</td>
        <td>{{ trip.destination }}</td>
        <td><button @click="selectTrip(trip.trip_id)">Show trip</button></td>
      </tr>
    </table>
    </form>
    </div>

    <h1>Trip</h1>
    <div v-if="singleTripData['trips'].length > 0">
      Trip id: {{ singleTripData['trips'][0].trip_id  }}<br>
      Route: {{ singleTripData['trips'][0].route_id  }} {{ singleTripData['trips'][0].direction_id }} to {{ singleTripData['trips'][0].destination }}

      <form @submit.prevent="">
      <table>
        <tr v-for="trip in singleTripData['trips']" :key="trip.stop_id">
          <td>{{ new Date(trip.interpolated_timestamp).toLocaleString() }}</td>
          <td>{{ trip.stop_name }}</td>
          <td>{{ (trip.pattern_distance / 5280.0).toFixed(2) }}mi</td>
          <td><button @click="selectStop(trip.stop_id, trip.route_id, trip.day)">Show stop</button></td>
        </tr>
      </table>
      </form>
    </div>
    <div v-if="stopData['stops'].length > 0">
    <h1>Stops</h1>
      <tr v-for="stop in stopData['stops']" :key="stop.trip_id">
          <td>{{ stop.trip_id }}</td>
          <td>{{ new Date(stop.interpolated_timestamp).toLocaleString() }}</td>
          <td>{{ stop.stop_name }}</td>
        </tr>
    </div>
  </div>
</template>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>
