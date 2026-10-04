import pathlib
import re

import relayplane


def test_the_version_is_written_once_and_the_package_metadata_agrees():
    pyproject = (pathlib.Path(__file__).parent.parent / "pyproject.toml").read_text(encoding="utf8")
    declared = re.search(r'(?m)^version\s*=\s*"([^"]+)"', pyproject).group(1)
    assert declared == relayplane.__version__


def test_the_user_agent_carries_the_real_version():
    from relayplane import RelayPlaneClient

    c = RelayPlaneClient("http://gw.test", "k")
    assert c._http._client.headers["User-Agent"] == f"relayplane-python/{relayplane.__version__}"
