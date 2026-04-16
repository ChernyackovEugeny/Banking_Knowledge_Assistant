import { BrowserRouter, Route, Routes } from 'react-router-dom'
import { ChatWindow } from './components/ChatWindow'
import { DashboardPage } from './pages/DashboardPage'

export default function App() {
  return (
    <BrowserRouter>
      <div className="h-full bg-gray-50 flex flex-col">
        <Routes>
          <Route path="/"          element={<ChatWindow />} />
          <Route path="/dashboard" element={<DashboardPage />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}
