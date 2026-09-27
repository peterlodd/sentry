import {css} from '@emotion/react';
import styled from '@emotion/styled';

import {Button} from '@sentry/scraps/button';
import {Hotkey, useHotkeys} from '@sentry/scraps/hotkey';
import {Container, Flex} from '@sentry/scraps/layout';

import {IconChevron} from 'sentry/icons/iconChevron';
import {t} from 'sentry/locale';
import {useOrganization} from 'sentry/utils/useOrganization';
import {useIssueDetails} from 'sentry/views/issueDetails/context';

const TOGGLE_SIDEBAR_HOTKEY = 'mod+alt+s';

/**
 * Registers the sidebar toggle hotkey. Render once per issue details page,
 * since `ToggleSidebar` buttons can appear in more than one place.
 */
export function ToggleSidebarHotkey() {
  const {isSidebarOpen, dispatch} = useIssueDetails();

  useHotkeys([
    {
      match: TOGGLE_SIDEBAR_HOTKEY,
      callback: () => dispatch({type: 'UPDATE_SIDEBAR_STATE', isOpen: !isSidebarOpen}),
    },
  ]);

  return null;
}

export function ToggleSidebar({size = 'md'}: {size?: 'md' | 'sm'}) {
  const organization = useOrganization();
  const {isSidebarOpen, dispatch} = useIssueDetails();
  const label = isSidebarOpen ? t('Close sidebar') : t('Open sidebar');

  return (
    <Container position="relative" display={{zero: 'none', '4xl': 'block'}}>
      <ToggleButton
        expanded={isSidebarOpen}
        onClick={() => dispatch({type: 'UPDATE_SIDEBAR_STATE', isOpen: !isSidebarOpen})}
        aria-label={label}
        tooltipProps={{
          title: (
            <Flex align="center" gap="sm">
              {label}
              <Hotkey value={TOGGLE_SIDEBAR_HOTKEY} />
            </Flex>
          ),
        }}
        style={size === 'md' ? undefined : {height: '26px'}}
        analyticsEventKey="issue_details.sidebar_toggle"
        analyticsEventName="Issue Details: Sidebar Toggle"
        analyticsParams={{
          sidebar_open: !isSidebarOpen,
          org_streamline_only: organization.streamlineOnly ?? undefined,
        }}
        icon={
          <IconChevron direction={isSidebarOpen ? 'right' : 'left'} isDouble size="xs" />
        }
      />
    </Container>
  );
}

// The extra 1px on width is to display above the sidebar border
const ToggleButton = styled(Button)<{expanded: boolean}>`
  ${p =>
    p.expanded &&
    css`
      margin-right: calc(-${p.theme.space.xl} - 1px);
      /* Square the right corners on both layers so the shadow (::before) reaches the edge like the surface (::after) */
      &::before,
      &::after {
        border-top-right-radius: 0px;
        border-bottom-right-radius: 0px;
      }
      &::after {
        border-right-color: transparent;
      }
    `}
`;
