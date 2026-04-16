// ──────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────

export interface OverviewData {
  total_requests: number
  ok_count: number
  error_count: number
  in_progress_count: number
  avg_latency_ms: number | null
  p95_latency_ms: number | null
  total_tokens_24h: number
  active_sessions_24h: number
  stuck_count: number
}

export interface TimelinePoint {
  hour: string     // "14:00"
  total: number
  errors: number
}

export interface TokenPoint {
  day: string      // "2026-04-15"
  total_tokens: number
  prompt_tokens: number
  completion_tokens: number
  calls: number
}

export interface RecentRequest {
  request_id: string
  session_short: string
  query_preview: string
  status: string
  total_duration_ms: number | null
  total_tokens: number | null
  retrieved_chunks_n: number | null
  requested_at: string | null
  error_msg: string | null
}

export interface StuckRequest {
  request_id: string
  session_short: string
  query: string
  minutes_ago: number
}

export interface SlowRequest {
  request_id: string
  session_short: string
  query: string
  total_duration_ms: number
}

export interface ErrorSession {
  session_short: string
  session_id: string
  request_count: number
  error_count: number
  error_pct: number
}

export interface AnomaliesData {
  stuck: StuckRequest[]
  slow: SlowRequest[]
  error_sessions: ErrorSession[]
}

export interface DocEntry {
  doc_id: string
  appearances: number
  unique_requests: number
  avg_score: number
  avg_rank: number
}

export interface ParseSession {
  session_id: string
  started_at: string | null
  duration_sec: number
  docs_total: number
  docs_ok: number
  docs_failed: number
  docs_skipped: number
  flag_force: boolean
  flag_only_filter: string | null
  is_running: boolean
}

export interface GenSession {
  session_id: string
  script_name: string
  started_at: string | null
  duration_sec: number
  docs_total: number
  docs_ok: number
  docs_failed: number
  docs_skipped: number
  flag_force: boolean
  is_running: boolean
}

export interface ValidationCheck {
  check_name: string
  artifact_type: string
  total: number
  passed: number
  pass_rate: number
}

export interface PipelineData {
  parse_sessions: ParseSession[]
  gen_sessions: GenSession[]
  validation_summary: ValidationCheck[]
}

// ──────────────────────────────────────────────────────────────────────────
// Fetch functions
// ──────────────────────────────────────────────────────────────────────────

const BASE = '/api/dashboard'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

export const fetchOverview   = () => get<OverviewData>('/overview')
export const fetchTimeline   = (hours = 24) => get<TimelinePoint[]>(`/chat/timeline?hours=${hours}`)
export const fetchTokens     = (days = 14)  => get<TokenPoint[]>(`/chat/tokens?days=${days}`)
export const fetchRecent     = (limit = 20) => get<RecentRequest[]>(`/chat/recent?limit=${limit}`)
export const fetchAnomalies  = () => get<AnomaliesData>('/chat/anomalies')
export const fetchTopDocs    = () => get<DocEntry[]>('/chat/docs')
export const fetchPipeline   = () => get<PipelineData>('/pipeline')
