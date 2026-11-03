import { BrowserRouter, Routes, Route, Link, NavLink, useLocation } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useSSE } from "./hooks/useSSE";
import { BrokerStatus } from "./components/BrokerStatus";
import { TaskList } from "./pages/TaskList";
import { TaskDetail } from "./pages/TaskDetail";
import { Search } from "./pages/Search";
import { InvocationDetail } from "./pages/InvocationDetail";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 500,
      refetchOnWindowFocus: false,
    },
  },
});

function Topbar({ streamConnected }: { streamConnected: boolean }) {
  const location = useLocation();
  const isSearch = location.pathname === "/search";

  return (
    <header className="topbar">
      <div className="topbar-left">
        <Link to="/" className="logo">
          <Logo />
          <span className="logo-text">
            <span className="logo-p">P</span>
            <span className="logo-h">h</span>
            <span className="logo-l">l</span>
            ower
          </span>
        </Link>
        <nav className="nav-tabs">
          {/* Task and invocation detail pages belong to the Tasks tab */}
          <NavLink
            to="/"
            className={`nav-tab${!isSearch ? " active" : ""}`}
            aria-current={!isSearch ? "page" : undefined}
          >
            Tasks
          </NavLink>
          <NavLink
            to="/search"
            className={`nav-tab${isSearch ? " active" : ""}`}
            aria-current={isSearch ? "page" : undefined}
          >
            Search
          </NavLink>
        </nav>
      </div>
      <BrokerStatus streamConnected={streamConnected} />
    </header>
  );
}

function Logo() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden>
      <circle cx="8" cy="8" r="2.2" fill="#F54E00" />
      <circle cx="8" cy="3.2" r="1.6" fill="#F5A623" />
      <circle cx="8" cy="12.8" r="1.6" fill="#F5A623" />
      <circle cx="3.2" cy="8" r="1.6" fill="#F5A623" />
      <circle cx="12.8" cy="8" r="1.6" fill="#F5A623" />
    </svg>
  );
}

function AppShell() {
  const streamConnected = useSSE();

  return (
    <>
      <Topbar streamConnected={streamConnected} />
      <main className="shell">
        <Routes>
          <Route path="/" element={<TaskList />} />
          <Route path="/tasks/:taskName" element={<TaskDetail />} />
          <Route path="/search" element={<Search />} />
          <Route path="/invocations/:taskId" element={<InvocationDetail />} />
        </Routes>
      </main>
    </>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppShell />
      </BrowserRouter>
    </QueryClientProvider>
  );
}
