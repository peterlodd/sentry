/**
 * Extracts the first human-readable error message from an API error response
 * body so that we can surface it to the user in a toast.
 *
 * Handles nested field validation errors such as:
 * `{statusDetails: {inNextRelease: ["No release data present..."]}}`
 */
export function getResponseErrorMessage(
  responseJSON: Record<string, unknown> | undefined
): string | undefined {
  if (!responseJSON) {
    return undefined;
  }
  return findFirstMessage(responseJSON);
}

function findFirstMessage(obj: Record<string, unknown>): string | undefined {
  for (const value of Object.values(obj)) {
    if (typeof value === 'string') {
      return value;
    }
    if (Array.isArray(value)) {
      if (typeof value[0] === 'string') {
        return value[0];
      }
      if (typeof value[0] === 'object' && value[0] !== null) {
        const nested = findFirstMessage(value[0] as Record<string, unknown>);
        if (nested) {
          return nested;
        }
      }
    }
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const nested = findFirstMessage(value as Record<string, unknown>);
      if (nested) {
        return nested;
      }
    }
  }
  return undefined;
}
