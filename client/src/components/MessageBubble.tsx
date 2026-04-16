import ReactMarkdown from 'react-markdown'
import type { Message } from '../types'
import { SourceCard } from './SourceCard'

interface Props {
  message: Message
}

export function MessageBubble({ message }: Props) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div className={`${isUser ? 'max-w-xl' : 'max-w-2xl w-full'}`}>
        {/* Аватар ассистента */}
        {!isUser && (
          <div className="flex items-center gap-2 mb-1.5">
            <div className="w-6 h-6 rounded-md bg-brand-700 flex items-center justify-center flex-shrink-0">
              <span className="text-white text-xs font-bold select-none">И</span>
            </div>
            <span className="text-xs text-gray-400 font-medium">КИБ Ассистент</span>
          </div>
        )}

        {/* Пузырь */}
        <div
          className={`rounded-2xl px-4 py-3 ${
            isUser
              ? 'bg-brand-700 text-white rounded-br-sm'
              : 'bg-white border border-gray-200 text-gray-800 rounded-bl-sm shadow-sm'
          }`}
        >
          {isUser ? (
            <p className="text-sm whitespace-pre-wrap leading-relaxed">
              {message.content}
            </p>
          ) : message.content ? (
            <div className="prose prose-sm max-w-none
                            prose-headings:text-brand-800 prose-headings:font-semibold
                            prose-a:text-brand-600 prose-strong:text-gray-900
                            prose-code:text-brand-700 prose-code:bg-brand-50 prose-code:px-1 prose-code:rounded
                            prose-li:my-0.5">
              <ReactMarkdown>{message.content}</ReactMarkdown>
            </div>
          ) : message.isStreaming ? (
            <ThinkingDots />
          ) : null}
        </div>

        {/* Источники */}
        {message.sources && message.sources.length > 0 && (
          <div className="mt-2 space-y-1.5">
            {message.sources.map((src, i) => (
              <SourceCard key={i} source={src} index={i + 1} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function ThinkingDots() {
  return (
    <div className="flex gap-1.5 py-1">
      {[0, 1, 2].map(i => (
        <span
          key={i}
          className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce"
          style={{ animationDelay: `${i * 0.15}s` }}
        />
      ))}
    </div>
  )
}
