export const AUTHOR_ONLY_FRONTMATTER_KEYS = Object.freeze(['social'])

export function stripAuthorOnlyFrontmatter(frontmatter) {
  if (!frontmatter || typeof frontmatter !== 'object') {
    return frontmatter
  }

  for (const key of AUTHOR_ONLY_FRONTMATTER_KEYS) {
    delete frontmatter[key]
  }
  return frontmatter
}

export function assertPublicProjection(value, context = 'public Coffee Machine data') {
  const visit = (candidate, path) => {
    if (!candidate || typeof candidate !== 'object') {
      return
    }
    for (const [key, child] of Object.entries(candidate)) {
      if (AUTHOR_ONLY_FRONTMATTER_KEYS.includes(key) || key === 'poll' || key === 'preferred') {
        throw new Error(`${context} contains author-only field ${path}.${key}`)
      }
      visit(child, `${path}.${key}`)
    }
  }

  visit(value, '$')
  return value
}
