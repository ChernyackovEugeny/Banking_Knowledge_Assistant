import { Link } from 'react-router-dom'
import { useChat } from '../hooks/useChat'
import { MessageList } from './MessageList'
import { InputBar } from './InputBar'

export function ChatWindow() {
  const { messages, isLoading, sendMessage } = useChat()

  return (
    <div className="flex flex-col h-full">
      {/* ── Шапка ── */}
      <header className="bg-brand-800 text-white px-5 py-3.5 flex items-center gap-3 shadow-md flex-shrink-0">
        <div className="w-9 h-9 rounded-lg bg-gold-500 flex items-center justify-center flex-shrink-0">
          <span className="text-brand-900 font-bold text-sm select-none">ИБ</span>
        </div>
        <div className="min-w-0">
          <h1 className="font-semibold text-base leading-tight">КИБ Ассистент</h1>
          <p className="text-brand-300 text-xs truncate">
            ПАО «ИнвестБанк» · корпоративный и инвестиционный банкинг
          </p>
        </div>
        <Link
          to="/dashboard"
          className="ml-auto text-brand-200 hover:text-white text-xs px-3 py-1 rounded border border-brand-600 hover:border-brand-400 transition-colors flex-shrink-0"
        >
          Дэшборд
        </Link>
      </header>

      {/* ── Сообщения ── */}
      <MessageList
        messages={messages}
        isLoading={isLoading}
        onExampleClick={sendMessage}
      />

      {/* ── Поле ввода ── */}
      <InputBar onSend={sendMessage} isLoading={isLoading} />
    </div>
  )
}
