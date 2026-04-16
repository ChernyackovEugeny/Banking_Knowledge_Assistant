import type { Source } from '../types'

export type StreamEvent =
  | { type: 'delta';   content: string }
  | { type: 'sources'; sources: Source[] }
  | { type: 'done';    full_text: string }

/**
 * Отправляет сообщение и читает SSE-стрим от /api/chat.
 * Использует async generator поверх Fetch API (поддерживает POST).
 */
export async function* streamChat(
  sessionId: string,
  message: string,
): AsyncGenerator<StreamEvent> {
  const response = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId, message }),
  })

  if (!response.ok || !response.body) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`)
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() ?? ''  // последняя строка может быть неполной

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue
      try {
        yield JSON.parse(line.slice(6)) as StreamEvent
      } catch {
        // пропускаем некорректные фреймы
      }
    }
  }
}
