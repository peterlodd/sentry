import {GroupFixture} from 'sentry-fixture/group';
import {GroupStatsFixture} from 'sentry-fixture/groupStats';
import {MemberFixture} from 'sentry-fixture/member';
import {ProjectFixture} from 'sentry-fixture/project';
import {TagsFixture} from 'sentry-fixture/tags';

import {act, render, screen, userEvent, waitFor} from 'sentry-test/reactTestingLibrary';
import {textWithMarkupMatcher} from 'sentry-test/utils';

import {PageFiltersStore} from 'sentry/components/pageFilters/store';
import {StreamGroup} from 'sentry/components/stream/group';
import {TagStore} from 'sentry/stores/tagStore';
import type {Group} from 'sentry/types/group';
import IssueList from 'sentry/views/issueList/overview';

jest.mock('sentry/views/issueList/filters', () => ({
  IssueListFilters: jest.fn(() => null),
}));
jest.mock('sentry/components/stream/group', () => ({
  __esModule: true,
  StreamGroup: jest.fn(({group}: {group: Group}) => <div data-test-id={group.id} />),
  LoadingStreamGroup: jest.fn(() => <div data-test-id="loading-group" />),
}));

jest.mock('js-cookie', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

const PREVIOUS_PAGE_CURSOR = '1443575731';
const DEFAULT_LINKS_HEADER =
  `<http://127.0.0.1:8000/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1>; rel="previous"; results="false"; cursor="${PREVIOUS_PAGE_CURSOR}:0:1", ` +
  '<http://127.0.0.1:8000/api/0/organizations/org-slug/issues/?cursor=1443575000:0:0>; rel="next"; results="true"; cursor="1443575000:0:0"';

describe('IssueList -> Polling', () => {
  let issuesRequest: jest.Mock;
  let pollRequest: jest.Mock;

  afterEach(() => {
    jest.useRealTimers();
    MockApiClient.clearMockResponses();
  });

  const NOW = new Date('2026-09-28T12:00:00.000Z');
  const project = ProjectFixture();
  // Use recent lastSeen so live-mode time-range pruning does not drop fixture rows.
  const group = GroupFixture({project, lastSeen: '2026-09-28T11:55:00.000Z'});
  const group2 = GroupFixture({
    project,
    id: '2',
    lastSeen: '2026-09-28T11:56:00.000Z',
  });

  /* helpers */
  const renderComponent = async () => {
    render(<IssueList />, {
      initialRouterConfig: {
        location: {
          pathname: '/organizations/org-slug/issues/',
          query: {query: 'is:unresolved'},
        },
      },
    });

    await act(async () => {
      await Promise.resolve();
      await jest.runAllTimersAsync();
    });

    // renderComponent may advance fake timers; pin wall clock for prune checks.
    jest.setSystemTime(NOW);
  };

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(NOW);

    MockApiClient.clearMockResponses();
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/sent-first-event/',
      body: {sentFirstEvent: true},
    });

    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/recent-searches/',
      body: [],
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/issues-count/',
      method: 'GET',
      body: [{}],
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/processingissues/',
      method: 'GET',
      body: [
        {
          project: 'test-project',
          numIssues: 1,
          hasIssues: true,
          lastSeen: '2019-01-16T15:39:11.081Z',
        },
      ],
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/tags/',
      method: 'GET',
      body: TagsFixture(),
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/users/',
      method: 'GET',
      body: [MemberFixture({projects: [project.slug]})],
    });

    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/recent-searches/',
      method: 'GET',
      body: [],
    });
    issuesRequest = MockApiClient.addMockResponse({
      url: '/organizations/org-slug/issues/',
      body: [group],
      headers: {
        Link: DEFAULT_LINKS_HEADER,
        'X-Hits': '1',
      },
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/issues-stats/',
      body: [
        GroupStatsFixture({
          id: group.id,
          lastSeen: group.lastSeen,
          firstSeen: '2026-09-28T11:00:00.000Z',
        }),
      ],
    });
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [],
      headers: {
        Link: DEFAULT_LINKS_HEADER,
        'X-Hits': '1',
      },
    });

    PageFiltersStore.onInitializeUrlState({
      projects: [parseInt(project.id, 10)],
      environments: [],
      datetime: {period: '14d', start: null, end: null, utc: null},
    });

    jest.mocked(StreamGroup).mockClear();
    TagStore.init();
  });

  it('toggles polling for new issues', async () => {
    await renderComponent();

    await waitFor(() => {
      expect(issuesRequest).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({
          // Should be called with default query
          data: expect.stringContaining('is%3Aunresolved'),
        })
      );
    });

    // Enable realtime updates
    await userEvent.click(
      screen.getByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    // Each poll request gets delayed by additional 3s, up to max of 60s
    await act(() => jest.advanceTimersByTimeAsync(3001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
    await act(() => jest.advanceTimersByTimeAsync(6001));
    expect(pollRequest).toHaveBeenCalledTimes(2);

    // Pauses
    await userEvent.click(screen.getByRole('button', {name: 'Pause real-time updates'}), {
      delay: null,
    });

    await act(() => jest.advanceTimersByTimeAsync(12001));
    expect(pollRequest).toHaveBeenCalledTimes(2);
  });

  it('displays new group and pagination caption correctly', async () => {
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [group2],
      headers: {
        Link: DEFAULT_LINKS_HEADER,
        'X-Hits': '2',
      },
    });

    await renderComponent();
    expect(
      await screen.findByText(textWithMarkupMatcher('1-1 of 1'))
    ).toBeInTheDocument();

    // Enable realtime updates
    await userEvent.click(
      screen.getByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    await act(() => jest.advanceTimersByTimeAsync(3001));
    expect(pollRequest).toHaveBeenCalledTimes(1);

    // We mock out the stream group component and only render the ID as a testid
    await screen.findByTestId('2');

    expect(screen.getByText(textWithMarkupMatcher('1-2 of 2'))).toBeInTheDocument();
  });

  it('removes issues that age out of the selected time range during live updates', async () => {
    const recentGroup = GroupFixture({
      project,
      id: '1',
      lastSeen: '2026-09-28T11:45:00.000Z',
    });
    const agingGroup = GroupFixture({
      project,
      id: '2',
      lastSeen: '2026-09-28T11:35:00.000Z',
    });

    PageFiltersStore.onInitializeUrlState({
      projects: [parseInt(project.id, 10)],
      environments: [],
      datetime: {period: '30m', start: null, end: null, utc: null},
    });

    issuesRequest = MockApiClient.addMockResponse({
      url: '/organizations/org-slug/issues/',
      body: [recentGroup, agingGroup],
      headers: {
        Link: DEFAULT_LINKS_HEADER,
        'X-Hits': '2',
      },
    });
    MockApiClient.addMockResponse({
      url: '/organizations/org-slug/issues-stats/',
      body: [
        GroupStatsFixture({
          id: recentGroup.id,
          lastSeen: recentGroup.lastSeen,
          firstSeen: '2026-09-28T11:00:00.000Z',
        }),
        GroupStatsFixture({
          id: agingGroup.id,
          lastSeen: agingGroup.lastSeen,
          firstSeen: '2026-09-28T11:00:00.000Z',
        }),
      ],
    });
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [],
      headers: {
        Link: DEFAULT_LINKS_HEADER,
        'X-Hits': '1',
      },
    });

    await renderComponent();

    expect(await screen.findByTestId('1')).toBeInTheDocument();
    expect(screen.getByTestId('2')).toBeInTheDocument();

    await userEvent.click(
      screen.getByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    // Advance wall clock so group 2 falls outside the 30m window, then poll.
    // Pin absolute time (do not rely on timer advancement for the window slide).
    jest.setSystemTime(new Date('2026-09-28T12:10:00.000Z'));
    await act(() => jest.advanceTimersByTimeAsync(3001));

    expect(pollRequest).toHaveBeenCalledTimes(1);
    await waitFor(() => {
      expect(screen.queryByTestId('2')).not.toBeInTheDocument();
    });
    expect(screen.getByTestId('1')).toBeInTheDocument();
    expect(screen.getByText(textWithMarkupMatcher('1-1 of 1'))).toBeInTheDocument();
  });

  it('stops polling for new issues when endpoint returns a 401', async () => {
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [],
      statusCode: 401,
    });

    await renderComponent();

    // Enable real time control
    await userEvent.click(
      await screen.findByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    // Each poll request gets delayed by additional 3s, up to max of 60s
    await act(() => jest.advanceTimersByTimeAsync(3001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
    await act(() => jest.advanceTimersByTimeAsync(9001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
  });

  it('stops polling for new issues when endpoint returns a 403', async () => {
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [],
      statusCode: 403,
    });

    await renderComponent();

    // Enable real time control
    await userEvent.click(
      await screen.findByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    // Each poll request gets delayed by additional 3s, up to max of 60s
    await act(() => jest.advanceTimersByTimeAsync(3001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
    await act(() => jest.advanceTimersByTimeAsync(9001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
  });

  it('stops polling for new issues when endpoint returns a 404', async () => {
    pollRequest = MockApiClient.addMockResponse({
      url: `/api/0/organizations/org-slug/issues/?cursor=${PREVIOUS_PAGE_CURSOR}:0:1`,
      body: [],
      statusCode: 404,
    });

    await renderComponent();

    // Enable real time control
    await userEvent.click(
      await screen.findByRole('button', {name: 'Enable real-time updates'}),
      {delay: null}
    );

    // Each poll request gets delayed by additional 3s, up to max of 60s
    await act(() => jest.advanceTimersByTimeAsync(3001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
    await act(() => jest.advanceTimersByTimeAsync(9001));
    expect(pollRequest).toHaveBeenCalledTimes(1);
  });
});
