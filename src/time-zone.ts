/**
 * Number of milliseconds in one minute.
 */
const MILLIS_PER_MINUTE = 60_000;

/**
 * Number of milliseconds in one day.
 */
const MILLIS_PER_DAY = 86_400_000;

/**
 * Local date and minute of the day of an instant in one time zone.
 */
export type LocalTime = {
  /**
   * Local date, `YYYY-MM-DD`.
   */
  readonly date: string;

  /**
   * Minutes since local midnight, `0` to `1439`.
   */
  readonly minute: number;
};

/**
 * Cached `Intl.DateTimeFormat` per time zone.
 */
const formatters = new Map<string, Intl.DateTimeFormat>();

/**
 * Returns the cached formatter giving numeric date and 24-hour time parts in `timeZone`. Throws a
 * `RangeError` for an unknown time zone.
 */
const formatterFor = (timeZone: string): Intl.DateTimeFormat => {
  const cached = formatters.get(timeZone);
  if (cached !== undefined) {
    return cached;
  }

  const created = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  });
  formatters.set(timeZone, created);
  return created;
};

/**
 * Returns `true` when `timeZone` is an IANA time zone this runtime knows.
 */
export const isValidTimeZone = (timeZone: string): boolean => {
  try {
    formatterFor(timeZone);
    return true;
  } catch {
    return false;
  }
};

/**
 * Local date and minute of the day of `ms` (epoch milliseconds) in `timeZone`.
 */
export const localTimeOf = (timeZone: string, ms: number): LocalTime => {
  const parts: Partial<Record<Intl.DateTimeFormatPartTypes, string>> = {};
  for (const part of formatterFor(timeZone).formatToParts(new Date(ms))) {
    parts[part.type] = part.value;
  }

  const year = (parts.year ?? '0').padStart(4, '0');
  const date = `${year}-${parts.month ?? '01'}-${parts.day ?? '01'}`;
  return { date, minute: Number(parts.hour ?? 0) * 60 + Number(parts.minute ?? 0) };
};

/**
 * Epoch milliseconds of local midnight of `date` (`YYYY-MM-DD`) read as if it were UTC.
 */
const naiveMidnightOf = (date: string): number => Date.parse(`${date}T00:00:00Z`);

/**
 * Offset of `timeZone` from UTC at `ms`, in whole minutes as milliseconds: the local wall time read
 * as UTC minus the instant, both truncated to the minute.
 */
const offsetAt = (timeZone: string, ms: number): number => {
  const flooredMs = Math.floor(ms / MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE;
  const local = localTimeOf(timeZone, flooredMs);
  return naiveMidnightOf(local.date) + local.minute * MILLIS_PER_MINUTE - flooredMs;
};

/**
 * Epoch milliseconds of the local time `minute` minutes after midnight of `date` in `timeZone`
 * (`1440` is the next local midnight). A time repeated by a daylight saving change resolves to its
 * later occurrence. A time skipped by one resolves as if the clock had not changed, which lands the
 * length of the gap after it (02:30 in a one-hour gap from 02:00 to 03:00 is 03:30 after it).
 * Returns `NaN` when `date` is not a `YYYY-MM-DD` date.
 */
export const latestInstantOf = (timeZone: string, date: string, minute: number): number => {
  const naiveMs = naiveMidnightOf(date) + minute * MILLIS_PER_MINUTE;
  if (Number.isNaN(naiveMs)) {
    return Number.NaN;
  }

  const offsetBefore = offsetAt(timeZone, naiveMs - MILLIS_PER_DAY);
  const offsetAfter = offsetAt(timeZone, naiveMs + MILLIS_PER_DAY);
  const candidates = [naiveMs - offsetBefore, naiveMs - offsetAfter].filter(
    (candidateMs) => candidateMs + offsetAt(timeZone, candidateMs) === naiveMs,
  );
  return candidates.length === 0 ? naiveMs - offsetBefore : Math.max(...candidates);
};
