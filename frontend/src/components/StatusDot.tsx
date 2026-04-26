interface Props {
  failRate: number;
  retryCount: number;
}

export function StatusDot({ failRate, retryCount }: Props) {
  const state =
    failRate >= 0.01 ? "bad" :
    retryCount > 0 || failRate >= 0.003 ? "warn" :
    "ok";

  return <span className={`status-dot ${state}`} aria-label={state} />;
}
