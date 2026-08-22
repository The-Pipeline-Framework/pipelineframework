import { createContentLoader } from 'vitepress'
import { buildPublicDataset } from '../../.vitepress/coffee-machine/content-model.js'

export default createContentLoader('value/coffee-machine/**/*.md', {
  transform: buildPublicDataset
})
