import { useState, useCallback, useRef } from 'react'
import { v4 as uuidv4 } from 'uuid'
import type { Message, Source } from '../types'
import { streamChat } from '../api/chat'

export function useChat() {
  const [messages, setMessages] = useState<Message[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const sessionId = useRef<string>(uuidv4()).current

  const sendMessage = useCallback(
    async (text: string) => {
      const trimmed = text.trim()
      if (!trimmed || isLoading) return

      const userMsg: Message = {
        id:        uuidv4(),
        role:      'user',
        content:   trimmed,
        timestamp: new Date(),
      }

      const botId = uuidv4()
      const botMsg: Message = {
        id:          botId,
        role:        'assistant',
        content:     '',
        timestamp:   new Date(),
        isStreaming: true,
      }

      setMessages(prev => [...prev, userMsg, botMsg])
      setIsLoading(true)

      try {
        for await (const event of streamChat(sessionId, trimmed)) {
          if (event.type === 'delta') {
            setMessages(prev =>
              prev.map(m =>
                m.id === botId
                  ? { ...m, content: m.content + event.content }
                  : m,
              ),
            )
          } else if (event.type === 'sources') {
            const sources: Source[] = event.sources
            setMessages(prev =>
              prev.map(m => (m.id === botId ? { ...m, sources } : m)),
            )
          } else if (event.type === 'done') {
            setMessages(prev =>
              prev.map(m =>
                m.id === botId ? { ...m, isStreaming: false } : m,
              ),
            )
          }
        }
      } catch {
        setMessages(prev =>
          prev.map(m =>
            m.id === botId
              ? {
                  ...m,
                  content:     'Ошибка соединения с сервером. Попробуйте ещё раз.',
                  isStreaming: false,
                }
              : m,
          ),
        )
      } finally {
        setIsLoading(false)
      }
    },
    [isLoading, sessionId],
  )

  return { messages, isLoading, sendMessage, sessionId }
}
