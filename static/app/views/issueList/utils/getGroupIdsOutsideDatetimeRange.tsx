import {DEFAULT_STATS_PERIOD} from 'sentry/constants';
import type {PageFilterDatetime} from 'sentry/types/core';
import {getDateFromTimestamp} from 'sentry/utils/dates';
import {getAbsoluteRangeFromPeriod} from 'sentry/utils/duration/getAbsoluteRangeFromPeriod';

type GroupWithLastSeen = {
  id: string;
  lastSeen: string;
};

function resolveDatetimeRange(
  datetime: PageFilterDatetime,
  now: number
): {end: Date; start: Date} | null {
  if (datetime.period) {
    return getAbsoluteRangeFromPeriod(datetime.period, now);
  }

  if (datetime.start) {
    const start = getDateFromTimestamp(datetime.start);
    if (!start) {
      return null;
    }
    const end = datetime.end ? getDateFromTimestamp(datetime.end) : new Date(now);
    if (!end) {
      return null;
    }
    return {start, end};
  }

  return getAbsoluteRangeFromPeriod(DEFAULT_STATS_PERIOD, now);
}

/**
 * Returns group IDs whose lastSeen falls outside the page-filter datetime
 * window. Used during live updates to drop issues that have aged out of a
 * relative stats period without re-running a full search.
 */
export function getGroupIdsOutsideDatetimeRange(
  groups: GroupWithLastSeen[],
  datetime: PageFilterDatetime,
  now: number = Date.now()
): string[] {
  const range = resolveDatetimeRange(datetime, now);
  if (!range) {
    return [];
  }

  const startMs = range.start.getTime();
  const endMs = range.end.getTime();

  return groups
    .filter(group => {
      const lastSeen = getDateFromTimestamp(group.lastSeen);
      if (!lastSeen) {
        return false;
      }
      const lastSeenMs = lastSeen.getTime();
      return lastSeenMs < startMs || lastSeenMs > endMs;
    })
    .map(group => group.id);
}
