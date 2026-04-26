import { BrowserRouter, Routes, Route, Link, useLocation, useNavigate } from "react-router-dom";
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

function Topbar() {
  const location = useLocation();
  const navigate = useNavigate();
  const isSearch = location.pathname === "/search";

  return (
    <div className="topbar">
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
        <div className="nav-tabs">
          <button
            className={`nav-tab${!isSearch ? " active" : ""}`}
            onClick={() => navigate("/")}
          >
            Tasks
          </button>
          <button
            className={`nav-tab${isSearch ? " active" : ""}`}
            onClick={() => navigate("/search")}
          >
            Search
          </button>
        </div>
      </div>
      <BrokerStatus />
    </div>
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
  useSSE();

  return (
    <>
      <Topbar />
      <div className="shell">
        <Routes>
          <Route path="/" element={<TaskList />} />
          <Route path="/tasks/:taskName" element={<TaskDetail />} />
          <Route path="/search" element={<Search />} />
          <Route path="/invocations/:taskId" element={<InvocationDetail />} />
        </Routes>
      </div>
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
