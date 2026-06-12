"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function AutoRefresh({ intervalMs = 1800, attempts = 6 }) {
  const router = useRouter();

  useEffect(() => {
    let remaining = Math.max(0, Number(attempts) || 0);
    if (remaining === 0) {
      return undefined;
    }

    const timer = window.setInterval(() => {
      remaining -= 1;
      router.refresh();
      if (remaining <= 0) {
        window.clearInterval(timer);
      }
    }, Math.max(1000, Number(intervalMs) || 1800));

    return () => window.clearInterval(timer);
  }, [attempts, intervalMs, router]);

  return null;
}
