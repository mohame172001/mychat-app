import * as React from "react"
import { Eye, EyeOff } from "lucide-react"

import { cn } from "@/lib/utils"
import { Input } from "./input"

const PasswordInput = React.forwardRef(({ className, inputClassName, ...props }, ref) => {
  const [visible, setVisible] = React.useState(false)
  const label = visible ? "Hide password" : "Show password"

  return (
    <div className={cn("relative", className)}>
      <Input
        ref={ref}
        type={visible ? "text" : "password"}
        className={cn("pe-10", inputClassName)}
        {...props}
      />
      <button
        type="button"
        aria-label={label}
        aria-controls={props.id}
        onClick={() => setVisible((value) => !value)}
        className="absolute end- top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 rounded-md"
      >
        {visible ? <EyeOff className="w-4 h-4" aria-hidden="true" /> : <Eye className="w-4 h-4" aria-hidden="true" />}
      </button>
    </div>
  )
})

PasswordInput.displayName = "PasswordInput"

export { PasswordInput }
