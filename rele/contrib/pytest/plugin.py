from unittest.mock import patch

import pytest
import rele


def pytest_addoption(parser):
    group = parser.getgroup("rele")
    group.addoption(
        "--name",
        action="store",
        dest="name",
        default="World",
        help='Default "name" for hello().',
    )


@pytest.fixture
def mock_rele_publish():
    print('Hello world from fixture!')
    with patch.object(rele, 'publish', autospec=True) as mock:
        yield mock
