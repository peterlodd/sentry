import {getResponseErrorMessage} from 'sentry/utils/getResponseErrorMessage';

describe('getResponseErrorMessage', () => {
  it('returns undefined for undefined', () => {
    expect(getResponseErrorMessage(undefined)).toBeUndefined();
  });

  it('returns undefined for empty object', () => {
    expect(getResponseErrorMessage({})).toBeUndefined();
  });

  it('handles {detail: "message"} shape', () => {
    expect(getResponseErrorMessage({detail: 'Something went wrong'})).toBe(
      'Something went wrong'
    );
  });

  it('handles {field: ["message"]} shape', () => {
    expect(getResponseErrorMessage({name: ['Name is required']})).toBe(
      'Name is required'
    );
  });

  it('handles nested statusDetails field errors', () => {
    expect(
      getResponseErrorMessage({
        statusDetails: {
          inNextRelease: [
            "No release data present in the system to form a basis for 'Next Release'",
          ],
        },
      })
    ).toBe("No release data present in the system to form a basis for 'Next Release'");
  });

  it('handles {dataSources: {field: ["message"]}} shape', () => {
    expect(getResponseErrorMessage({dataSources: {query: ['Invalid query']}})).toBe(
      'Invalid query'
    );
  });

  it('handles {actions: [{field: "message"}]} shape', () => {
    expect(
      getResponseErrorMessage({
        actions: [{repo: 'Repository is required'}],
      })
    ).toBe('Repository is required');
  });

  it('returns the first message when multiple fields have errors', () => {
    const result = getResponseErrorMessage({
      name: ['Name error'],
      query: ['Query error'],
    });
    expect(result).toBe('Name error');
  });
});
