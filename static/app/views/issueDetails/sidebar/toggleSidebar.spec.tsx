import {render, screen, userEvent} from 'sentry-test/reactTestingLibrary';

import {IssueDetailsContextProvider} from 'sentry/views/issueDetails/context';
import {
  ToggleSidebar,
  ToggleSidebarHotkey,
} from 'sentry/views/issueDetails/sidebar/toggleSidebar';

describe('ToggleSidebar', () => {
  it('toggles the sidebar with mod+alt+s', async () => {
    render(
      <IssueDetailsContextProvider>
        <ToggleSidebarHotkey />
        <ToggleSidebar />
      </IssueDetailsContextProvider>
    );

    expect(screen.getByRole('button', {name: 'Close sidebar'})).toBeInTheDocument();

    await userEvent.keyboard('{Control>}{Alt>}s{/Alt}{/Control}');
    expect(screen.getByRole('button', {name: 'Open sidebar'})).toBeInTheDocument();

    await userEvent.keyboard('{Control>}{Alt>}s{/Alt}{/Control}');
    expect(screen.getByRole('button', {name: 'Close sidebar'})).toBeInTheDocument();
  });
});
