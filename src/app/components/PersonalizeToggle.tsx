"use client";
import { useEffect, useState } from "react";

declare global {
  interface Window {
    kidflixSetPersonalize?: (on: boolean) => void;
  }
}

export default function PersonalizeToggle() {
  // Initialize from localStorage synchronously to avoid UI flicker
  const [on, setOn] = useState<boolean>(() => {
    try {
      return localStorage.getItem("kidflix_mode") === "personalized";
    } catch {
      return false;
    }
  });

  // On mount, push current state to df-messenger (in case it loaded first)
  useEffect(() => {
    try {
      window.kidflixSetPersonalize?.(on);
    } catch {}
  }, []); // run once

  // Keep messenger + storage in sync whenever user toggles
  useEffect(() => {
    try {
      localStorage.setItem("kidflix_mode", on ? "personalized" : "ann");
    } catch {}
    window.kidflixSetPersonalize?.(on);
  }, [on]);

  // OPTIONAL: listen for external changes to kidflix_mode (e.g., another UI)
  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === "kidflix_mode" && typeof e.newValue === "string") {
        setOn(e.newValue === "personalized");
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  return (
    <label
      className="inline-flex items-center gap-2 cursor-pointer select-none"
      role="switch"
      aria-checked={on}
    >
      <input
        type="checkbox"
        checked={on}
        onChange={(e) => setOn(e.target.checked)}
        className="cursor-pointer"
        aria-label="Personalize results"
      />
      <span>Personalize results</span>
    </label>
  );
}
