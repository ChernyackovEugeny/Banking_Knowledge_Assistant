import { useCallback, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  AnomaliesData, DocEntry, GenSession, OverviewData,
  ParseSession, PipelineData, RecentRequest, TimelinePoint,
  TokenPoint, ValidationCheck,
  fetchAnomalies, fetchOverview, fetchPipeline, fetchRecent,
  fetchTimeline, fetchTokens, fetchTopDocs,
} from '../api/dashboard'

// ──────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────

function fmtMs(ms: number | null): string {
  if (ms == null) return '—'
  if (ms < 1000) return `${Math.round(ms)} мс`
  return `${(ms / 1000).toFixed(1)} с`
}

function fmtTokens(n: number | null): string {
  if (n == null) return '—'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

function fmtDate(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleString('ru-RU', {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  })
}

function fmtDay(iso: string): string {
  if (!iso) return ''
  const [, month, day] = iso.split('-')
  return `${day}.${month}`
}

// ──────────────────────────────────────────────────────────────────────────
// Small UI components
// ──────────────────────────────────────────────────────────────────────────

interface StatCardProps {
  label: string
  value: string
  sub?: string
  accent?: 'default' | 'danger' | 'warning' | 'ok'
}

function StatCard({ label, value, sub, accent = 'default' }: StatCardProps) {
  const accentClass = {
    default: 'border-brand-200',
    danger:  'border-red-400',
    warning: 'border-amber-400',
    ok:      'border-emerald-400',
  }[accent]

  const valueClass = {
    default: 'text-brand-800',
    danger:  'text-red-600',
    warning: 'text-amber-600',
    ok:      'text-emerald-600',
  }[accent]

  return (
    <div className={`bg-white rounded-xl border-l-4 ${accentClass} px-4 py-3 shadow-sm`}>
      <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${valueClass}`}>{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    ok:          'bg-emerald-100 text-emerald-700',
    error:       'bg-red-100 text-red-700',
    in_progress: 'bg-amber-100 text-amber-700',
    skipped:     'bg-gray-100 text-gray-500',
    failed:      'bg-red-100 text-red-700',
    manual_pdf:  'bg-blue-100 text-blue-700',
  }
  const label: Record<string, string> = {
    ok:          'ok',
    error:       'error',
    in_progress: 'processing',
    skipped:     'skipped',
    failed:      'failed',
    manual_pdf:  'manual pdf',
  }
  const cls = styles[status] ?? 'bg-gray-100 text-gray-600'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {label[status] ?? status}
    </span>
  )
}

function PassRateBar({ rate }: { rate: number }) {
  const color = rate >= 80 ? 'bg-emerald-500' : rate >= 50 ? 'bg-amber-400' : 'bg-red-500'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-200 rounded-full h-2">
        <div className={`${color} h-2 rounded-full transition-all`} style={{ width: `${rate}%` }} />
      </div>
      <span className="text-xs text-gray-600 w-10 text-right">{rate}%</span>
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h2 className="text-sm font-semibold text-brand-800 uppercase tracking-wide mb-3">{children}</h2>
}

function EmptyState({ msg }: { msg: string }) {
  return (
    <div className="text-center py-8 text-gray-400 text-sm">{msg}</div>
  )
}

// ──────────────────────────────────────────────────────────────────────────
// Tab: Chat
// ──────────────────────────────────────────────────────────────────────────

interface ChatTabProps {
  overview: OverviewData | null
  timeline: TimelinePoint[]
  tokens: TokenPoint[]
  recent: RecentRequest[]
  anomalies: AnomaliesData | null
  topDocs: DocEntry[]
}

function ChatTab({ overview, timeline, tokens, recent, anomalies, topDocs }: ChatTabProps) {
  const errorRate = overview
    ? overview.total_requests > 0
      ? ((overview.error_count / overview.total_requests) * 100).toFixed(1)
      : '0.0'
    : null

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        <StatCard
          label="Запросов (24ч)"
          value={overview ? String(overview.total_requests) : '—'}
          sub={overview ? `${overview.ok_count} успешных` : undefined}
        />
        <StatCard
          label="Ошибки"
          value={errorRate != null ? `${errorRate}%` : '—'}
          sub={overview ? `${overview.error_count} запросов` : undefined}
          accent={
            overview && overview.error_count === 0 ? 'ok'
            : overview && parseFloat(errorRate ?? '0') > 5 ? 'danger'
            : 'warning'
          }
        />
        <StatCard
          label="Avg латентность"
          value={overview ? fmtMs(overview.avg_latency_ms) : '—'}
          sub="от запроса до done"
        />
        <StatCard
          label="P95 латентность"
          value={overview ? fmtMs(overview.p95_latency_ms) : '—'}
          sub="95-й перцентиль"
          accent={overview && (overview.p95_latency_ms ?? 0) > 30000 ? 'warning' : 'default'}
        />
        <StatCard
          label="Токены (24ч)"
          value={overview ? fmtTokens(overview.total_tokens_24h) : '—'}
          sub="только успешные"
        />
        <StatCard
          label="Сессий (24ч)"
          value={overview ? String(overview.active_sessions_24h) : '—'}
          sub={overview && overview.stuck_count > 0
            ? `⚠ ${overview.stuck_count} зависших`
            : 'нет аномалий'}
          accent={overview && overview.stuck_count > 0 ? 'warning' : 'default'}
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Timeline */}
        <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
          <SectionTitle>Запросы по часам (24ч)</SectionTitle>
          {timeline.length === 0 ? (
            <EmptyState msg="Нет данных за последние 24 часа" />
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={timeline} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="hour" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
                <Tooltip
                  contentStyle={{ fontSize: 12, borderRadius: 8 }}
                  formatter={(v, name) => [v, name === 'total' ? 'Всего' : 'Ошибки']}
                />
                <Legend formatter={(v) => v === 'total' ? 'Всего' : 'Ошибки'} wrapperStyle={{ fontSize: 12 }} />
                <Line type="monotone" dataKey="total"  stroke="#2869cd" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="errors" stroke="#ef4444" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Tokens */}
        <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
          <SectionTitle>Расход токенов (14 дней)</SectionTitle>
          {tokens.length === 0 ? (
            <EmptyState msg="Нет данных о расходе токенов" />
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={tokens.map(t => ({ ...t, day: fmtDay(t.day) }))}
                        margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="day" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => v >= 1000 ? `${Math.round(v/1000)}K` : v} />
                <Tooltip
                  contentStyle={{ fontSize: 12, borderRadius: 8 }}
                  formatter={(v, name) => [
                    typeof v === 'number' ? v.toLocaleString('ru-RU') : v,
                    name === 'prompt_tokens' ? 'Промпт' : name === 'completion_tokens' ? 'Ответ' : 'Всего',
                  ]}
                />
                <Legend
                  formatter={(v) => v === 'prompt_tokens' ? 'Промпт' : 'Ответ'}
                  wrapperStyle={{ fontSize: 12 }}
                />
                <Bar dataKey="prompt_tokens"     stackId="a" fill="#7ea5e1" />
                <Bar dataKey="completion_tokens" stackId="a" fill="#2869cd" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Anomalies */}
      {anomalies && (anomalies.stuck.length > 0 || anomalies.slow.length > 0 || anomalies.error_sessions.length > 0) ? (
        <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 space-y-4">
          <SectionTitle>⚠ Аномалии</SectionTitle>

          {anomalies.stuck.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-amber-700 mb-2">
                Зависшие запросы (in_progress &gt; 5 мин)
              </p>
              <div className="space-y-1">
                {anomalies.stuck.map(r => (
                  <div key={r.request_id} className="flex gap-3 text-xs bg-white rounded px-3 py-2 border border-amber-100">
                    <span className="text-gray-400 font-mono w-16 shrink-0">{r.session_short}…</span>
                    <span className="text-amber-700 font-medium w-20 shrink-0">{r.minutes_ago} мин назад</span>
                    <span className="text-gray-600 truncate">{r.query}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {anomalies.slow.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-orange-700 mb-2">
                Медленные ответы (&gt; 30 с, последние 24ч)
              </p>
              <div className="space-y-1">
                {anomalies.slow.map(r => (
                  <div key={r.request_id} className="flex gap-3 text-xs bg-white rounded px-3 py-2 border border-orange-100">
                    <span className="text-gray-400 font-mono w-16 shrink-0">{r.session_short}…</span>
                    <span className="text-orange-700 font-medium w-20 shrink-0">{fmtMs(r.total_duration_ms)}</span>
                    <span className="text-gray-600 truncate">{r.query}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {anomalies.error_sessions.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-red-700 mb-2">
                Проблемные сессии
              </p>
              <div className="space-y-1">
                {anomalies.error_sessions.map(s => (
                  <div key={s.session_id} className="flex gap-3 text-xs bg-white rounded px-3 py-2 border border-red-100">
                    <span className="text-gray-400 font-mono w-16 shrink-0">{s.session_short}…</span>
                    <span className="text-red-600 font-medium w-24 shrink-0">{s.error_pct}% ошибок</span>
                    <span className="text-gray-500">{s.error_count} из {s.request_count} запросов</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : anomalies ? (
        <div className="bg-emerald-50 border border-emerald-200 rounded-xl px-4 py-3 text-sm text-emerald-700">
          ✓ Аномалий не обнаружено
        </div>
      ) : null}

      {/* Recent requests */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-4 pt-4 pb-2">
          <SectionTitle>Последние запросы</SectionTitle>
        </div>
        {recent.length === 0 ? (
          <EmptyState msg="Запросов ещё не было" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 text-gray-500 uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">Время</th>
                  <th className="px-4 py-2 text-left font-medium">Сессия</th>
                  <th className="px-4 py-2 text-left font-medium">Запрос</th>
                  <th className="px-4 py-2 text-left font-medium">Статус</th>
                  <th className="px-4 py-2 text-right font-medium">Латентность</th>
                  <th className="px-4 py-2 text-right font-medium">Токены</th>
                  <th className="px-4 py-2 text-right font-medium">Чанки</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {recent.map(r => (
                  <tr
                    key={r.request_id}
                    className={`hover:bg-gray-50 transition-colors ${
                      r.status === 'error' ? 'bg-red-50' :
                      r.status === 'in_progress' ? 'bg-amber-50' : ''
                    }`}
                  >
                    <td className="px-4 py-2 text-gray-400 whitespace-nowrap font-mono">
                      {fmtDate(r.requested_at)}
                    </td>
                    <td className="px-4 py-2 text-gray-400 font-mono">{r.session_short}…</td>
                    <td className="px-4 py-2 text-gray-700 max-w-xs">
                      <span className="line-clamp-1">{r.query_preview}</span>
                      {r.error_msg && (
                        <span className="block text-red-500 truncate">{r.error_msg}</span>
                      )}
                    </td>
                    <td className="px-4 py-2"><StatusBadge status={r.status} /></td>
                    <td className="px-4 py-2 text-right text-gray-600 whitespace-nowrap">
                      {fmtMs(r.total_duration_ms)}
                    </td>
                    <td className="px-4 py-2 text-right text-gray-600">
                      {r.total_tokens != null ? r.total_tokens.toLocaleString('ru-RU') : '—'}
                    </td>
                    <td className="px-4 py-2 text-right text-gray-600">
                      {r.retrieved_chunks_n ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Top docs */}
      {topDocs.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-4 pt-4 pb-2">
            <SectionTitle>Топ документов в контексте LLM</SectionTitle>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 text-gray-500 uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">Документ</th>
                  <th className="px-4 py-2 text-right font-medium">Появлений</th>
                  <th className="px-4 py-2 text-right font-medium">Уникальных запросов</th>
                  <th className="px-4 py-2 text-right font-medium">Avg Score</th>
                  <th className="px-4 py-2 text-right font-medium">Avg Rank</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {topDocs.map(d => (
                  <tr key={d.doc_id} className="hover:bg-gray-50">
                    <td className="px-4 py-2 font-mono font-medium text-brand-700">{d.doc_id}</td>
                    <td className="px-4 py-2 text-right text-gray-700">{d.appearances}</td>
                    <td className="px-4 py-2 text-right text-gray-600">{d.unique_requests}</td>
                    <td className="px-4 py-2 text-right text-gray-500">
                      {d.avg_score > 0 ? d.avg_score.toFixed(4) : '—'}
                    </td>
                    <td className="px-4 py-2 text-right text-gray-500">
                      {d.avg_rank > 0 ? d.avg_rank.toFixed(1) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────
// Tab: Parsing
// ──────────────────────────────────────────────────────────────────────────

function ParseSessionRow({ s }: { s: ParseSession }) {
  return (
    <tr className="hover:bg-gray-50 text-xs">
      <td className="px-4 py-2 text-gray-400 font-mono whitespace-nowrap">{fmtDate(s.started_at)}</td>
      <td className="px-4 py-2">
        {s.is_running
          ? <span className="text-amber-600 font-medium">▶ запущен</span>
          : <span className="text-gray-500">{s.duration_sec.toFixed(1)} с</span>}
      </td>
      <td className="px-4 py-2 text-right text-gray-700">{s.docs_total}</td>
      <td className="px-4 py-2 text-right text-emerald-600 font-medium">{s.docs_ok}</td>
      <td className="px-4 py-2 text-right text-red-500 font-medium">{s.docs_failed > 0 ? s.docs_failed : '—'}</td>
      <td className="px-4 py-2 text-right text-gray-400">{s.docs_skipped > 0 ? s.docs_skipped : '—'}</td>
      <td className="px-4 py-2 text-gray-400 text-center">
        {s.flag_force && <span className="bg-orange-100 text-orange-600 px-1.5 py-0.5 rounded text-xs">--force</span>}
      </td>
      <td className="px-4 py-2 text-gray-400 font-mono truncate max-w-xs">
        {s.flag_only_filter
          ? <span className="text-brand-600">{s.flag_only_filter}</span>
          : <span className="text-gray-300">все</span>}
      </td>
    </tr>
  )
}

function ParsingTab({ sessions }: { sessions: ParseSession[] }) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-4 pt-4 pb-2">
          <SectionTitle>Запуски парсинга (последние 10)</SectionTitle>
        </div>
        {sessions.length === 0 ? (
          <EmptyState msg="Парсинг ещё не запускался" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 text-gray-500 uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">Старт</th>
                  <th className="px-4 py-2 text-left font-medium">Время</th>
                  <th className="px-4 py-2 text-right font-medium">Всего</th>
                  <th className="px-4 py-2 text-right font-medium">OK</th>
                  <th className="px-4 py-2 text-right font-medium">Ошибок</th>
                  <th className="px-4 py-2 text-right font-medium">Пропущено</th>
                  <th className="px-4 py-2 text-center font-medium">Флаги</th>
                  <th className="px-4 py-2 text-left font-medium">--only</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sessions.map(s => <ParseSessionRow key={s.session_id} s={s} />)}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────
// Tab: Generation
// ──────────────────────────────────────────────────────────────────────────

function GenSessionRow({ s }: { s: GenSession }) {
  const scriptColor: Record<string, string> = {
    generating: 'bg-brand-100 text-brand-700',
    questions:  'bg-purple-100 text-purple-700',
    validator:  'bg-teal-100 text-teal-700',
  }
  return (
    <tr className="hover:bg-gray-50 text-xs">
      <td className="px-4 py-2 text-gray-400 font-mono whitespace-nowrap">{fmtDate(s.started_at)}</td>
      <td className="px-4 py-2">
        <span className={`px-2 py-0.5 rounded text-xs font-medium ${scriptColor[s.script_name] ?? 'bg-gray-100 text-gray-600'}`}>
          {s.script_name}
        </span>
      </td>
      <td className="px-4 py-2">
        {s.is_running
          ? <span className="text-amber-600 font-medium">▶ запущен</span>
          : <span className="text-gray-500">{s.duration_sec.toFixed(1)} с</span>}
      </td>
      <td className="px-4 py-2 text-right text-gray-700">{s.docs_total}</td>
      <td className="px-4 py-2 text-right text-emerald-600 font-medium">{s.docs_ok}</td>
      <td className="px-4 py-2 text-right text-red-500 font-medium">{s.docs_failed > 0 ? s.docs_failed : '—'}</td>
      <td className="px-4 py-2 text-gray-400 text-center">
        {s.flag_force && <span className="bg-orange-100 text-orange-600 px-1.5 py-0.5 rounded text-xs">--force</span>}
      </td>
    </tr>
  )
}

function ValidationSummary({ checks }: { checks: ValidationCheck[] }) {
  if (checks.length === 0) return null

  const documents = checks.filter(c => c.artifact_type === 'document')
  const questions = checks.filter(c => c.artifact_type === 'questions')

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
      <SectionTitle>Последний запуск validator.py</SectionTitle>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {[['Документы', documents], ['Вопросы (Q&A)', questions]] .map(([title, list]) => (
          (list as ValidationCheck[]).length > 0 && (
            <div key={title as string}>
              <p className="text-xs font-semibold text-gray-500 mb-2">{title as string}</p>
              <div className="space-y-2">
                {(list as ValidationCheck[]).map(c => (
                  <div key={c.check_name}>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-700 font-medium">{c.check_name}</span>
                      <span className="text-gray-500">{c.passed}/{c.total}</span>
                    </div>
                    <PassRateBar rate={c.pass_rate} />
                  </div>
                ))}
              </div>
            </div>
          )
        ))}
      </div>
    </div>
  )
}

function GenerationTab({ sessions, validation }: { sessions: GenSession[], validation: ValidationCheck[] }) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-4 pt-4 pb-2">
          <SectionTitle>Запуски генерации (последние 10)</SectionTitle>
        </div>
        {sessions.length === 0 ? (
          <EmptyState msg="Генерация ещё не запускалась" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 text-gray-500 uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">Старт</th>
                  <th className="px-4 py-2 text-left font-medium">Скрипт</th>
                  <th className="px-4 py-2 text-left font-medium">Время</th>
                  <th className="px-4 py-2 text-right font-medium">Всего</th>
                  <th className="px-4 py-2 text-right font-medium">OK</th>
                  <th className="px-4 py-2 text-right font-medium">Ошибок</th>
                  <th className="px-4 py-2 text-center font-medium">Флаги</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sessions.map(s => <GenSessionRow key={s.session_id} s={s} />)}
              </tbody>
            </table>
          </div>
        )}
      </div>
      <ValidationSummary checks={validation} />
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────
// Main DashboardPage
// ──────────────────────────────────────────────────────────────────────────

type Tab = 'chat' | 'parsing' | 'generation'

export function DashboardPage() {
  const [tab, setTab] = useState<Tab>('chat')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const [overview, setOverview]   = useState<OverviewData | null>(null)
  const [timeline, setTimeline]   = useState<TimelinePoint[]>([])
  const [tokens, setTokens]       = useState<TokenPoint[]>([])
  const [recent, setRecent]       = useState<RecentRequest[]>([])
  const [anomalies, setAnomalies] = useState<AnomaliesData | null>(null)
  const [topDocs, setTopDocs]     = useState<DocEntry[]>([])
  const [pipeline, setPipeline]   = useState<PipelineData | null>(null)

  const pipelineLoadedRef = useRef(false)

  const fetchChatData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [ov, tl, tk, rc, an, td] = await Promise.all([
        fetchOverview(),
        fetchTimeline(),
        fetchTokens(),
        fetchRecent(),
        fetchAnomalies(),
        fetchTopDocs(),
      ])
      setOverview(ov)
      setTimeline(tl)
      setTokens(tk)
      setRecent(rc)
      setAnomalies(an)
      setTopDocs(td)
      setLastUpdated(new Date())
    } catch (e) {
      setError('Не удалось загрузить данные. Проверьте подключение к PostgreSQL и статус API.')
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchPipelineData = useCallback(async () => {
    if (pipelineLoadedRef.current) return
    try {
      const data = await fetchPipeline()
      setPipeline(data)
      pipelineLoadedRef.current = true
    } catch {
      // Non-critical: show empty state
    }
  }, [])

  // Initial load
  useEffect(() => { fetchChatData() }, [fetchChatData])

  // Auto-refresh every 30s when on chat tab
  useEffect(() => {
    if (tab !== 'chat') return
    const id = setInterval(fetchChatData, 30_000)
    return () => clearInterval(id)
  }, [tab, fetchChatData])

  // Lazy-load pipeline data
  useEffect(() => {
    if (tab === 'parsing' || tab === 'generation') {
      fetchPipelineData()
    }
  }, [tab, fetchPipelineData])

  const tabs: { id: Tab; label: string }[] = [
    { id: 'chat',       label: 'Чат' },
    { id: 'parsing',    label: 'Парсинг' },
    { id: 'generation', label: 'Генерация' },
  ]

  return (
    <div className="min-h-full bg-gray-50 flex flex-col">
      {/* Header */}
      <header className="bg-brand-800 text-white px-5 py-3.5 flex items-center gap-3 shadow-md flex-shrink-0">
        <div className="w-9 h-9 rounded-lg bg-gold-500 flex items-center justify-center flex-shrink-0">
          <span className="text-brand-900 font-bold text-sm select-none">ИБ</span>
        </div>
        <div className="min-w-0">
          <h1 className="font-semibold text-base leading-tight">КИБ Ассистент · Дэшборд</h1>
          <p className="text-brand-300 text-xs truncate">
            ПАО «ИнвестБанк» · логи и аналитика
          </p>
        </div>
        <div className="ml-auto flex items-center gap-3">
          {lastUpdated && (
            <span className="text-brand-300 text-xs hidden sm:block">
              {lastUpdated.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
          )}
          <button
            onClick={() => { pipelineLoadedRef.current = false; fetchChatData(); fetchPipelineData() }}
            disabled={loading}
            className="text-brand-200 hover:text-white text-sm px-3 py-1 rounded border border-brand-600 hover:border-brand-400 transition-colors disabled:opacity-40"
          >
            {loading ? '…' : '↻ Обновить'}
          </button>
          <Link
            to="/"
            className="text-brand-200 hover:text-white text-sm px-3 py-1 rounded border border-brand-600 hover:border-brand-400 transition-colors"
          >
            Чат
          </Link>
        </div>
      </header>

      {/* Tabs */}
      <div className="bg-white border-b border-gray-200 px-5">
        <div className="flex gap-1">
          {tabs.map(t => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                tab === t.id
                  ? 'border-brand-600 text-brand-700'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <main className="flex-1 p-5 max-w-7xl mx-auto w-full">
        {error ? (
          <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm">
            {error}
          </div>
        ) : tab === 'chat' ? (
          <ChatTab
            overview={overview}
            timeline={timeline}
            tokens={tokens}
            recent={recent}
            anomalies={anomalies}
            topDocs={topDocs}
          />
        ) : tab === 'parsing' ? (
          <ParsingTab sessions={pipeline?.parse_sessions ?? []} />
        ) : (
          <GenerationTab
            sessions={pipeline?.gen_sessions ?? []}
            validation={pipeline?.validation_summary ?? []}
          />
        )}
      </main>
    </div>
  )
}
