const stateClass: Record<string, string> = {
  SUCCESS: "st-success",
  FAILURE: "st-failure",
  RETRY: "st-retry",
  STARTED: "st-active",
  RECEIVED: "st-pending",
  REVOKED: "st-revoked",
};

interface Props {
  state: string;
  small?: boolean;
}

export function Badge({ state, small }: Props) {
  return (
    <span className={`badge ${small ? "sm" : ""} ${stateClass[state] || ""}`}>
      {state}
    </span>
  );
}
