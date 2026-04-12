import test from 'node:test'
import assert from 'node:assert/strict'

import {normalizeDevToFeed} from '../functions/api/devto-feed.js'

test('normalizeDevToFeed returns normalized feed items', () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <item>
      <title>Architectural mobility &amp; stronger software</title>
      <dc:creator>Mariano Barcia</dc:creator>
      <pubDate>Tue, 07 Apr 2026 19:37:36 +0000</pubDate>
      <link>https://dev.to/mbarcia/architectural-mobility-for-stronger-software-2nh4</link>
      <guid>https://dev.to/mbarcia/architectural-mobility-for-stronger-software-2nh4</guid>
      <description>&lt;p&gt;Mobility matters.&lt;/p&gt;</description>
    </item>
    <item>
      <title>Second item</title>
      <dc:creator>TPF</dc:creator>
      <pubDate>Wed, 08 Apr 2026 20:00:00 +0000</pubDate>
      <link>https://dev.to/mbarcia/second</link>
      <guid>https://dev.to/mbarcia/second</guid>
      <description>&lt;p&gt;Second excerpt.&lt;/p&gt;</description>
    </item>
  </channel>
</rss>`

  const result = normalizeDevToFeed(xml, 10)

  assert.deepEqual(result, [
    {
      id: 'https://dev.to/mbarcia/architectural-mobility-for-stronger-software-2nh4',
      title: 'Architectural mobility & stronger software',
      url: 'https://dev.to/mbarcia/architectural-mobility-for-stronger-software-2nh4',
      published_at: '2026-04-07T19:37:36.000Z',
      author: 'Mariano Barcia',
      excerpt: 'Mobility matters.'
    },
    {
      id: 'https://dev.to/mbarcia/second',
      title: 'Second item',
      url: 'https://dev.to/mbarcia/second',
      published_at: '2026-04-08T20:00:00.000Z',
      author: 'TPF',
      excerpt: 'Second excerpt.'
    }
  ])
})

test('normalizeDevToFeed strips HTML and truncates excerpts cleanly', () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <item>
      <title>HTML excerpt</title>
      <dc:creator>TPF</dc:creator>
      <pubDate>Wed, 08 Apr 2026 20:00:00 +0000</pubDate>
      <link>https://dev.to/mbarcia/html-excerpt</link>
      <guid>https://dev.to/mbarcia/html-excerpt</guid>
      <description>&lt;p&gt;First &lt;strong&gt;paragraph&lt;/strong&gt; with markup.&lt;/p&gt;&lt;p&gt;${'word '.repeat(60)}&lt;/p&gt;</description>
    </item>
  </channel>
</rss>`

  const [article] = normalizeDevToFeed(xml, 10)

  assert.match(article.excerpt, /^First paragraph with markup\./)
  assert.ok(article.excerpt.endsWith('...'))
  assert.ok(article.excerpt.length <= 183)
})

test('normalizeDevToFeed tolerates missing author and invalid dates', () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Missing author</title>
      <pubDate>not-a-date</pubDate>
      <link>https://dev.to/mbarcia/missing-author</link>
      <guid>https://dev.to/mbarcia/missing-author</guid>
      <description>&lt;p&gt;Excerpt&lt;/p&gt;</description>
    </item>
  </channel>
</rss>`

  const [article] = normalizeDevToFeed(xml, 10)

  assert.equal(article.author, '')
  assert.equal(article.published_at, '')
  assert.equal(article.title, 'Missing author')
})

test('normalizeDevToFeed returns an empty array for malformed XML', () => {
  assert.deepEqual(normalizeDevToFeed('<rss><channel><item>', 10), [])
  assert.deepEqual(normalizeDevToFeed('not xml', 10), [])
})
