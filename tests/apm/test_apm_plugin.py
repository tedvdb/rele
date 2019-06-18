from unittest.mock import patch, ANY, Mock

import pytest
from elasticapm.traces import Span

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
def active_span_mock():
    with patch('rele.contrib.apm_middleware.Tracer.active_span') as mock:
        mock.return_value = Mock(spec=Span)
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


@pytest.mark.usefixtures('instrument_mock', 'active_span_mock')
class TestAPMPlugin:
    @pytest.mark.usefixtures(
        'carrier_get_trace_parent_mock', 'apm_middleware')
    def test_span_is_started_on_pre_publish(
            self, publisher, start_active_span_mock):
        message = {'foo': 'bar'}
        topic = 'order-cancelled'
        publisher.publish(topic=topic, data=message, myattr='hello')

        start_active_span_mock.assert_called_with(
            topic, finish_on_close=False)

    @pytest.mark.usefixtures('apm_middleware')
    def test_apm_tracer_id_is_injected_in_message_attributes_on_pre_publish(
            self, publisher):
        publisher.publish(
            topic='order-cancelled',
            data={'foo': 'bar'},
            myattr='hello'
        )
        call_args = publisher._client.publish.call_args

        assert ELASTIC_APM_TRACE_PARENT in call_args[1].keys()

    @pytest.mark.usefixtures('apm_middleware', 'start_active_span_mock')
    def test_message_is_published_even_when_apm_fails(
            self, instrument_mock, publisher):
        instrument_mock.side_effect = Exception('Something went wrong on APM')
        publisher.publish(
            topic='order-cancelled',
            data={'foo': 'bar'},
            myattr='hello'
        )

        assert publisher._client.publish.called_once()

    def test_message_is_published_even_when_apm_fails_setting_up_APM(
            self, publisher, config):
        with patch('rele.contrib.apm_middleware.'
                   'Client.__init__') as client_mock:
            client_mock.side_effect = Exception(
                'Something went wrong initializing APM'
            )
            config.middleware = ['rele.contrib.APMMiddleware']
            register_middleware(config)

            publisher.publish(
                topic='order-cancelled',
                data={'foo': 'bar'},
                myattr='hello'
            )

            assert publisher._client.publish.called_once()

    @pytest.mark.usefixtures('apm_middleware')
    def test_span_is_finished_after_message_is_published(
            self, publisher, active_span_mock):
        publisher.publish(
            topic='order-cancelled',
            data={'foo': 'bar'},
            myattr='hello'
        )

        active_span_mock.finish.assert_called()
