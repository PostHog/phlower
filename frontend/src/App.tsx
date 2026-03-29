import { BrowserRouter, Routes, Route, Link } from "react-router-dom";
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

function AppShell() {
  useSSE();

  return (
    <>
      <nav className="topbar">
        <Link to="/" className="logo">Phlower</Link>
        <div className="nav-links">
          <Link to="/">Tasks</Link>
          <Link to="/search">Search</Link>
        </div>
        <BrokerStatus />
      </nav>
      <main>
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
