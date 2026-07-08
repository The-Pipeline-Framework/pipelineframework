<script setup>
import {nextTick, onMounted, onUnmounted, watch} from 'vue'
import {useRoute} from 'vitepress'

const route = useRoute()
const passiveClickOptions = {passive: true}
let suppressedSidebarClicks = 0
let routeSyncTimer
let sidebarClickTimer

function directSidebarItemChildren(parent) {
  return Array.from(parent?.children ?? [])
    .filter((child) => child.classList?.contains('VPSidebarItem'))
}

function closeSidebarItem(item) {
  if (!item || item.classList.contains('collapsed')) {
    return
  }

  const toggle = item.querySelector(':scope > .item > .caret')
    ?? item.querySelector(':scope > .item')
  if (!toggle) {
    return
  }

  suppressedSidebarClicks += 1
  toggle.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}))
}

function collapseOpenSiblings(item) {
  const parent = item?.parentElement
  if (!parent) {
    return
  }

  for (const sibling of directSidebarItemChildren(parent)) {
    if (
      sibling !== item
      && sibling.classList.contains('collapsible')
      && !sibling.classList.contains('collapsed')
    ) {
      closeSidebarItem(sibling)
    }
  }
}

function syncActiveGroups() {
  for (const activeItem of document.querySelectorAll('.VPSidebarItem.collapsible.has-active')) {
    collapseOpenSiblings(activeItem)
  }
}

function scheduleActiveSync() {
  window.clearTimeout(routeSyncTimer)
  nextTick(() => {
    // VitePress updates active/collapsed sidebar classes shortly after route render.
    routeSyncTimer = window.setTimeout(() => {
      syncActiveGroups()
      syncNavMenuGroups()
    }, 60)
  })
}

function onSidebarClick(event) {
  if (suppressedSidebarClicks > 0) {
    suppressedSidebarClicks -= 1
    return
  }

  if (!(event.target instanceof Element)) {
    return
  }

  const clickedItem = event.target.closest('.VPSidebarItem.collapsible > .item')
  const sidebarItem = clickedItem?.parentElement
  if (!sidebarItem?.classList.contains('VPSidebarItem')) {
    return
  }

  window.clearTimeout(sidebarClickTimer)
  sidebarClickTimer = window.setTimeout(() => {
    if (!sidebarItem.classList.contains('collapsed')) {
      collapseOpenSiblings(sidebarItem)
    }
  }, 0)
}

function navMenuGroupChildren(parent) {
  return Array.from(parent?.children ?? [])
    .filter((child) => child.classList?.contains('VPMenuGroup'))
}

function directNavMenuGroup(eventTarget) {
  if (!(eventTarget instanceof Element)) {
    return null
  }

  const title = eventTarget.closest('.VPNavBarMenuGroup .VPMenuGroup > .title')
  return title?.parentElement?.classList.contains('VPMenuGroup')
    ? title.parentElement
    : null
}

function setNavMenuGroupOpen(group, open) {
  group.classList.add('tpf-nav-accordion')
  group.classList.toggle('tpf-nav-accordion-collapsed', !open)
}

function collapseOpenNavMenuSiblings(group) {
  for (const sibling of navMenuGroupChildren(group.parentElement)) {
    if (sibling !== group) {
      setNavMenuGroupOpen(sibling, false)
    }
  }
}

function syncNavMenuGroups() {
  for (const group of document.querySelectorAll('.VPNavBarMenuGroup .VPMenuGroup')) {
    const hasActiveLink = Boolean(group.querySelector('.VPLink.active'))
    setNavMenuGroupOpen(group, hasActiveLink)
    if (hasActiveLink) {
      collapseOpenNavMenuSiblings(group)
    }
  }
}

function onNavMenuClick(event) {
  const group = directNavMenuGroup(event.target)
  if (!group) {
    return
  }

  const shouldOpen = group.classList.contains('tpf-nav-accordion-collapsed')
  setNavMenuGroupOpen(group, shouldOpen)
  if (shouldOpen) {
    collapseOpenNavMenuSiblings(group)
  }
}

onMounted(() => {
  document.addEventListener('click', onSidebarClick, passiveClickOptions)
  document.addEventListener('click', onNavMenuClick, passiveClickOptions)
  scheduleActiveSync()
})

onUnmounted(() => {
  window.clearTimeout(routeSyncTimer)
  window.clearTimeout(sidebarClickTimer)
  document.removeEventListener('click', onSidebarClick, passiveClickOptions)
  document.removeEventListener('click', onNavMenuClick, passiveClickOptions)
})

watch(
  () => route.path,
  scheduleActiveSync
)
</script>

<template></template>

<style>
.VPNavBarMenuGroup .VPMenuGroup.tpf-nav-accordion > .title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  cursor: pointer;
  user-select: none;
}

.VPNavBarMenuGroup .VPMenuGroup.tpf-nav-accordion > .title::after {
  content: "";
  flex: 0 0 auto;
  width: 8px;
  height: 8px;
  border-right: 1.5px solid currentColor;
  border-bottom: 1.5px solid currentColor;
  transform: rotate(45deg);
  transition: transform 0.18s ease;
}

.VPNavBarMenuGroup .VPMenuGroup.tpf-nav-accordion-collapsed > .title::after {
  transform: rotate(-45deg);
}

.VPNavBarMenuGroup .VPMenuGroup.tpf-nav-accordion-collapsed > :not(.title) {
  display: none;
}
</style>
