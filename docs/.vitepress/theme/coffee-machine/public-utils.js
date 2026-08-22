export function chooseDifferentIndex(length, currentIndex, random = Math.random) {
  if (length <= 1) return 0
  const candidate = Math.floor(random() * (length - 1))
  return candidate >= currentIndex ? candidate + 1 : candidate
}

export function normalizeSearch(value) {
  return String(value || '').trim().toLocaleLowerCase('en-US')
}

export function searchableFaqText(faq, personaLookup) {
  return normalizeSearch([
    faq.question,
    faq.quote,
    ...faq.tags,
    ...faq.personas.flatMap((entry) => {
      const persona = personaLookup.get(entry.persona)
      return [entry.text, persona?.name, persona?.description, persona?.worldview]
    })
  ].filter(Boolean).join(' '))
}
