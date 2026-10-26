import * as React from "react"
import { cn } from "../lib/utils"

// Self-contained shadcn-style Separator. Deliberately NO
// @radix-ui/react-separator: Radix augments the host app's global CSSProperties
// with `--radix-*` keys (breaking unrelated typed-style code in Kasal), and a
// decorative rule needs none of Radix's behaviour. A plain div with role keeps
// this embeddable while preserving the same look + a11y semantics.
export interface SeparatorProps extends React.HTMLAttributes<HTMLDivElement> {
  orientation?: "horizontal" | "vertical"
  decorative?: boolean
}

const Separator = React.forwardRef<HTMLDivElement, SeparatorProps>(
  ({ className, orientation = "horizontal", decorative = true, ...props }, ref) => (
    <div
      ref={ref}
      role={decorative ? "none" : "separator"}
      aria-orientation={decorative ? undefined : orientation}
      className={cn(
        "shrink-0 bg-border",
        orientation === "horizontal" ? "h-px w-full" : "h-full w-px",
        className,
      )}
      {...props}
    />
  ),
)
Separator.displayName = "Separator"

export { Separator }
