interface Props {
  values: number[];
  width?: number;
  height?: number;
}

export function Sparkline({ values, width = 80, height = 20 }: Props) {
  const peak = Math.max(...values, 1);
  const n = values.length;
  if (n === 0) return <svg width={width} height={height} />;

  const points = values
    .map((v, i) => {
      const x = (i / Math.max(n - 1, 1)) * width;
      const y = height - (v / peak) * (height - 2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  const fill = `0,${height} ${points} ${width},${height}`;

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
      <polyline points={fill} fill="var(--blue)" fillOpacity={0.08} stroke="none" />
      <polyline
        points={points}
        fill="none"
        stroke="var(--blue)"
        strokeWidth={1.2}
        strokeLinejoin="round"
      />
    </svg>
  );
}
