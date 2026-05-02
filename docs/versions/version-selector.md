# Version Selector

```js
export default {
  data() {
    return {
      versions: [
        { name: 'v26.5.1 (latest)', url: '/', current: true },
        { name: 'v26.5.1 (snapshot)', url: '/versions/v26.5.1/', current: false },
        { name: 'v26.4.4', url: '/versions/v26.4.4/', current: false },
        { name: 'v26.4.3', url: '/versions/v26.4.3/', current: false },
        { name: 'v26.2', url: '/versions/v26.2/', current: false },
        { name: 'v0.9.2', url: '/versions/v0.9.2/', current: false },
        { name: 'v0.9.0', url: '/versions/v0.9.0/', current: false }
      ]
    }
  },
  template: `
    <div class="version-selector">
      <label for="version-select">Documentation Version:</label>
      <select id="version-select" @change="changeVersion">
        <option v-for="version in versions" :value="version.url" :selected="version.current">
          {{ version.name }} {{ version.current ? '(current)' : '' }}
        </option>
      </select>
    </div>
  `,
  methods: {
    changeVersion(event) {
      window.location.href = event.target.value
    }
  }
}
```

This component allows users to switch between documentation versions easily.
