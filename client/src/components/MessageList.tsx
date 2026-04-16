import { useEffect, useRef } from 'react'
import type { Message } from '../types'
import { MessageBubble } from './MessageBubble'

const EXAMPLES = [
  'Кто одобряет кредит на 1,5 млрд рублей?',
  'Сроки KYC-идентификации корпоративных клиентов',
  'Порядок эскалации подозрительных операций',
  'Что такое норматив Н6 и чему он равен?',
]

interface Props {
  messages:       Message[]
  isLoading:      boolean
  onExampleClick: (text: string) => void
}

export function MessageList({ messages, isLoading, onExampleClick }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  if (messages.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-center px-6 overflow-y-auto messages-scroll">
        <div className="w-16 h-16 rounded-2xl bg-brand-100 flex items-center justify-center mb-4">
          <span className="text-3xl select-none">📋</span>
        </div>
        <h2 className="text-brand-800 font-semibold text-xl mb-2">
          Чем могу помочь?
        </h2>
        <p className="text-gray-500 text-sm max-w-md leading-relaxed">
          Задайте вопрос по внутренним регламентам, нормативным актам Банка России,
          процедурам KYC, кредитованию или операциям с ценными бумагами.
        </p>
        <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-xl w-full">
          {EXAMPLES.map((q, i) => (
            <button
              key={i}
              disabled={isLoading}
              onClick={() => onExampleClick(q)}
              className="text-left text-sm bg-white border border-gray-200 rounded-xl px-4 py-3
                         text-gray-600 hover:border-brand-400 hover:text-brand-700 hover:bg-brand-50
                         disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {q}
            </button>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-y-auto messages-scroll px-4 py-6 space-y-5">
      {messages.map(msg => (
        <MessageBubble key={msg.id} message={msg} />
      ))}
      <div ref={bottomRef} />
    </div>
  )
}
