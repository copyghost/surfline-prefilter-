export function batchRanges(total: number, size: number) {
  const ranges: Array<{ start: number; end: number }> = [];
  const batchSize = Math.max(1, size);

  for (let start = 0; start < total; start += batchSize) {
    ranges.push({ start, end: Math.min(start + batchSize, total) });
  }

  return ranges;
}
