import {DEFAULT_STATS_PERIOD} from 'sentry/constants';
import {getAbsoluteRangeFromPeriod} from 'sentry/utils/duration/getAbsoluteRangeFromPeriod';
import {getGroupIdsOutsideDatetimeRange} from 'sentry/views/issueList/utils/getGroupIdsOutsideDatetimeRange';

describe('getGroupIdsOutsideDatetimeRange', () => {
  const now = new Date('2026-09-28T12:00:00.000Z').getTime();

  it('returns IDs whose lastSeen is outside a relative period', () => {
    const groups = [
      {id: 'in-range', lastSeen: '2026-09-28T11:45:00.000Z'},
      {id: 'too-old', lastSeen: '2026-09-28T11:00:00.000Z'},
      {id: 'too-new', lastSeen: '2026-09-28T12:30:00.000Z'},
    ];

    expect(
      getGroupIdsOutsideDatetimeRange(
        groups,
        {period: '30m', start: null, end: null, utc: null},
        now
      )
    ).toEqual(['too-old', 'too-new']);
  });

  it('returns IDs outside an absolute start/end range', () => {
    const groups = [
      {id: 'in-range', lastSeen: '2026-09-28T11:00:00.000Z'},
      {id: 'too-old', lastSeen: '2026-09-28T09:00:00.000Z'},
    ];

    expect(
      getGroupIdsOutsideDatetimeRange(
        groups,
        {
          period: null,
          start: '2026-09-28T10:00:00.000Z',
          end: '2026-09-28T12:00:00.000Z',
          utc: true,
        },
        now
      )
    ).toEqual(['too-old']);
  });

  it('falls back to DEFAULT_STATS_PERIOD when datetime is empty', () => {
    const range = getAbsoluteRangeFromPeriod(DEFAULT_STATS_PERIOD, now);
    expect(range).not.toBeNull();

    const groups = [
      {id: 'in-range', lastSeen: new Date(now - 60_000).toISOString()},
      {
        id: 'too-old',
        lastSeen: new Date(range!.start.getTime() - 60_000).toISOString(),
      },
    ];

    expect(
      getGroupIdsOutsideDatetimeRange(
        groups,
        {period: null, start: null, end: null, utc: null},
        now
      )
    ).toEqual(['too-old']);
  });

  it('keeps groups with unparseable lastSeen', () => {
    expect(
      getGroupIdsOutsideDatetimeRange(
        [{id: 'bad-date', lastSeen: 'not-a-date'}],
        {period: '30m', start: null, end: null, utc: null},
        now
      )
    ).toEqual([]);
  });
});
