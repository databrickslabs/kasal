import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

// shadcn/ui class merge helper: dedupes/overrides Tailwind classes safely.
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
