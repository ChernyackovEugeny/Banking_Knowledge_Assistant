import { useState, useRef, type KeyboardEvent } from 'react'

interface Props {
  onSend:    (text: string) => void
  isLoading: boolean
}

export function InputBar({ onSend, isLoading }: Props) {
  const [value, setValue] = useState('')
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const handleSend = () => {
    const text = value.trim()
    if (!text || isLoading) return
    onSend(text)
    setValue('')
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
    }
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const handleInput = () => {
    const el = textareaRef.current
    if (!el) return
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`
  }

  return (
    <div className="border-t border-gray-200 bg-white px-4 py-3 flex-shrink-0">
      <div className="max-w-3xl mx-auto flex items-end gap-3">
        {/* Поле ввода */}
        <div className="flex-1 bg-gray-100 rounded-2xl px-4 py-2.5 flex items-end gap-2">
          <textarea
            ref={textareaRef}
            value={value}
            onChange={e => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onInput={handleInput}
            placeholder="Задайте вопрос по документам банка..."
            rows={1}
            disabled={isLoading}
            className="flex-1 bg-transparent resize-none outline-none text-sm text-gray-800
                       placeholder-gray-400 max-h-40 disabled:opacity-50 leading-relaxed"
          />
          <span className="text-xs text-gray-400 self-end pb-0.5 flex-shrink-0 hidden sm:block">
            Enter ↵
          </span>
        </div>

        {/* Кнопка отправки */}
        <button
          onClick={handleSend}
          disabled={isLoading || !value.trim()}
          title="Отправить"
          className="w-10 h-10 rounded-xl bg-brand-700 text-white flex items-center justify-center
                     hover:bg-brand-600 disabled:opacity-40 disabled:cursor-not-allowed
                     transition-colors flex-shrink-0"
        >
          {isLoading ? (
            <span className="w-4 h-4 border-2 border-white/40 border-t-white rounded-full animate-spin" />
          ) : (
            <SendIcon />
          )}
        </button>
      </div>

      <p className="text-center text-xs text-gray-400 mt-2 select-none">
        ИнвестБанк КИБ Ассистент · Только для служебного пользования
      </p>
    </div>
  )
}

function SendIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M22 2L11 13" />
      <path d="M22 2L15 22l-4-9-9-4 20-7z" />
    </svg>
  )
}
