<template>
  Main vue<br>
  Str: {{ hellostr }}
</template>

<script setup>
  import { ref } from 'vue';

  const hellostr = ref('Hello!')

  function setPosition(position) {
    console.log('position is ', position)
    hellostr.value = '' + position.coords.latitude + ', ' + position.coords.longitude
  }

  function setHello() {
    if (navigator.geolocation) {
      //const position =
      console.log('position')
      navigator.geolocation.getCurrentPosition(setPosition)
      console.log('done')
    } else {
      hellostr.value = 'no location available'
      console.log('no position')
    }
  }

  setHello()
</script>

<script>
export default {
  data() {
    return {
      hellostr: 'Hello2 !'
    }
  },
}
</script>
