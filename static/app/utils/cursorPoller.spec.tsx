import {CursorPoller} from 'sentry/utils/cursorPoller';

describe('CursorPoller', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    MockApiClient.clearMockResponses();
  });

  afterEach(() => {
    jest.useRealTimers();
    MockApiClient.clearMockResponses();
  });

  it('invokes success with an empty list when a poll returns no new issues', async () => {
    const success = jest.fn();
    MockApiClient.addMockResponse({
      url: '/api/0/organizations/org-slug/issues/?cursor=0:0:1',
      body: [],
      headers: {
        'X-Hits': '3',
      },
    });

    const poller = new CursorPoller({
      linkPreviousHref:
        'http://127.0.0.1:8000/api/0/organizations/org-slug/issues/?cursor=0:0:1',
      success,
    });
    poller.enable();

    await jest.advanceTimersByTimeAsync(3001);

    expect(success).toHaveBeenCalledWith([], {queryCount: 3});

    poller.disable();
  });
});
