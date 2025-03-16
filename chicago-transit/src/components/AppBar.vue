<template>
  <v-app-bar>
    <div style="align-items: center; display: flex">
      <v-btn
        variant="elevated"
        @click="routeHome()"
      >
        Home
      </v-btn>
      <v-btn
        variant="elevated"
        @click="refresh()"
      >
        Refresh
      </v-btn>
    </div>
  </v-app-bar>
</template>

<script setup>
  import { useRouter } from 'vue-router';

  const router = useRouter();

  function routeHome() {
    router.push('/');
  }

  function refresh() {
    router.go();
  }

</script>
