interface Props {
  state: string;
  small?: boolean;
}

export function Badge({ state }: Props) {
  const cls =
    state === "SUCCESS" ? "success" :
    state === "FAILURE" ? "failure" :
    state === "RETRY" ? "retry" :
    state === "STARTED" ? "started" :
    "received";

  return <span className={`state-badge ${cls}`}>{state}</span>;
}

export function StateLabel({ state }: { state: string }) {
  const cls =
    state === "SUCCESS" ? "success" :
    state === "FAILURE" ? "failure" :
    state === "RETRY" ? "retry" :
    state === "STARTED" ? "started" :
    "received";

  return <span className={`state-label ${cls}`}>{state}</span>;
}
