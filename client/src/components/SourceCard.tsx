import { useState } from 'react'
import type { Source } from '../types'

interface Props {
  source: Source
  index:  number
}

export function SourceCard({ source, index }: Props) {
  const [open, setOpen] = useState(false)

  return (
    <button
      onClick={() => setOpen(v => !v)}
      className="w-full text-left bg-brand-50 border border-brand-100 rounded-xl px-3 py-2
                 hover:bg-brand-100 transition-colors"
    >
      <div className="flex items-center gap-2">
        <span className="text-xs font-semibold text-brand-600 bg-brand-100 rounded px-1.5 py-0.5 flex-shrink-0">
          [{index}]
        </span>
        <span className="text-xs text-brand-800 font-medium truncate">
          {source.title || source.doc_id}
        </span>
        {source.section && (
          <span className="text-xs text-gray-400 truncate hidden sm:block">
            {source.section}
          </span>
        )}
        <span className="ml-auto text-gray-400 text-xs flex-shrink-0">
          {open ? '▲' : '▼'}
        </span>
      </div>
      {open && source.section && (
        <p className="mt-2 text-xs text-gray-600 border-t border-brand-200 pt-2 text-left">
          {source.section}
        </p>
      )}
    </button>
  )
}
