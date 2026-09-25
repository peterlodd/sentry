from sentry.integrations.slack.utils.escape import (
    escape_slack_link_text,
    escape_slack_text,
)


def test_escape_slack_text() -> None:
    assert escape_slack_text(None) == ""
    assert escape_slack_text("") == ""
    assert escape_slack_text("a & b <c> d") == "a &amp; b &lt;c&gt; d"


def test_escape_slack_link_text_collapses_whitespace() -> None:
    assert escape_slack_link_text(None) == ""
    assert escape_slack_link_text("") == ""
    assert (
        escape_slack_link_text("User Feedback: the app freezes on checkout\n\nreproduced twice")
        == "User Feedback: the app freezes on checkout reproduced twice"
    )
    assert escape_slack_link_text("  hello\t\nworld  ") == "hello world"


def test_escape_slack_link_text_escapes_special_chars() -> None:
    assert escape_slack_link_text("a & b\n<c>") == "a &amp; b &lt;c&gt;"
