import {getResponseErrorMessage} from 'sentry/utils/getResponseErrorMessage';

/**
 * Extracts the first human-readable error message from a workflow engine API
 * error response so that we can surface it to the user in a toast.
 */
export function getWorkflowEngineResponseErrorMessage(
  responseJSON: Record<string, unknown> | undefined
): string | undefined {
  return getResponseErrorMessage(responseJSON);
}
