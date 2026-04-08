import os
import pytest


def test_get_connection_params_reads_env_vars(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
    monkeypatch.setenv("SNOWFLAKE_USER", "test-user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test-pass")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test-wh")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "test-role")

    from utils.session import get_connection_params
    params = get_connection_params()

    assert params["account"] == "test-account"
    assert params["user"] == "test-user"
    assert params["password"] == "test-pass"
    assert params["warehouse"] == "test-wh"
    assert params["role"] == "test-role"


def test_get_connection_params_raises_on_missing_env():
    for key in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE"]:
        os.environ.pop(key, None)

    with pytest.raises(KeyError):
        from utils.session import get_connection_params
        get_connection_params()
