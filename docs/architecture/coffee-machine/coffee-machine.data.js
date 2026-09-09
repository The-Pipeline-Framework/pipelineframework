import { createContentLoader } from 'vitepress'
import { buildPublicDataset } from '../../.vitepress/coffee-machine/content-model.js'

export default createContentLoader('architecture/coffee-machine/**/*.md', {
  transform: buildPublicDataset
})
