"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function AutoRefresh({ intervalMs = 1800, attempts = 6, refreshKey = "" }) {
  const router = useRouter();

  useEffect(() => {
    let remaining = Math.max(0, Number(attempts) || 0);
    if (remaining === 0) {
      return undefined;
    }

    router.refresh();

    const timer = window.setInterval(() => {
      remaining -= 1;
      router.refresh();
      if (remaining <= 0) {
        window.clearInterval(timer);
      }
    }, Math.max(600, Number(intervalMs) || 1800));

    return () => window.clearInterval(timer);
  }, [attempts, intervalMs, refreshKey, router]);

  return null;
}
