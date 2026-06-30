import pytest

from t2c_data.core.config import settings


@pytest.fixture(autouse=True)
def _enable_datalake_default_env_credentials(request, monkeypatch):
    """The Data Lake 'default_environment' auth mode is opt-in in production
    (DATALAKE_ALLOW_DEFAULT_ENV_CREDENTIALS, default off). The data_lake tests exercise that
    mode directly, so enable it just for those modules."""
    if "data_lake" in request.module.__name__:
        monkeypatch.setattr(settings, "datalake_allow_default_env_credentials", True)
