---
search: false
---

# Version Selector

```js
export default {
  data() {
    return {
      versions: [
        { name: 'v0.9.2', url: '/', current: true },
        { name: 'v0.8.0', url: '/v0.8.0/', current: false },
        { name: 'v0.7.0', url: '/v0.7.0/', current: false }
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