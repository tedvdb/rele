from unittest.mock import patch, ANY

import pytest

from rele.contrib.apm_middleware import ELASTIC_APM_TRACE_PARENT
from rele.middleware import register_middleware


@pytest.fixture()
def apm_middleware(config):
    config.middleware = ['rele.contrib.APMMiddleware']
    register_middleware(config)


@pytest.fixture
def start_active_span_mock():
    with patch('rele.contrib.apm_middleware.Tracer.start_active_span') as mock:
        yield mock


@pytest.fixture
def instrument_mock():
    with patch('rele.contrib.apm_middleware.elasticapm.instrument') as mock:
        yield mock


@pytest.fixture
def carrier_get_trace_parent_mock():
    with patch('rele.contrib.apm_middleware.Carrier.get_trace_parent') as mock:
        mock.return_value = '1234'
        yield mock


@pytest.mark.usefixtures('instrument_mock', 'apm_middleware')
class TestAPMPlugin:
    @pytest.mark.usefixtures('carrier_get_trace_parent_mock')
    def test_span_is_started_on_pre_publish(
            self, publisher, start_active_span_mock):
        message = {'foo': 'bar'}
        topic = 'order-cancelled'
        publisher.publish(topic=topic, data=message, myattr='hello')

        start_active_span_mock.assert_called_with(
            topic, finish_on_close=False)

    def test_apm_tracer_id_is_injected_in_message_attributes_on_pre_publish(
            self, publisher):
        publisher.publish(
            topic='order-cancelled',
            data={'foo': 'bar'},
            myattr='hello'
        )
        call_args = publisher._client.publish.call_args

        assert ELASTIC_APM_TRACE_PARENT in call_args[1].keys()


