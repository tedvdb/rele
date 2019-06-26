import logging

import elasticapm
from elasticapm import Client
from elasticapm.contrib.opentracing import Tracer
from opentracing import Format

from rele.middleware import BaseMiddleware

ELASTIC_APM_TRACE_PARENT = 'elastic-apm-traceparent'
logger = logging.getLogger(__name__)


class Carrier(dict):
    def get_trace_parent(self):
        return str(self.get(ELASTIC_APM_TRACE_PARENT), "utf-8")


class APMMiddleware(BaseMiddleware):
    _tracer = None
    _carrier = None

    def setup(self, config):
        try:
            apm_client = Client({'SERVICE_NAME': config.gc_project_id})
            self._tracer = Tracer(apm_client)
            self._carrier = Carrier()
        except Exception as e:
            logger.warning(f'APM client could not be initialized. {e}')

    def pre_publish(self, topic, data, attrs):
        try:
            scope = self._tracer.start_active_span(topic, finish_on_close=False)
            self._inject_trace_parent(attrs, scope)
        except Exception as e:
            logger.warning(f'APM tracer could not start instrumentation. {e}')

    def _inject_trace_parent(self, attrs, scope):
        self._tracer.inject(span_context=scope.span.context,
                            format=Format.TEXT_MAP,
                            carrier=self._carrier)
        attrs[ELASTIC_APM_TRACE_PARENT] = self._carrier.get_trace_parent()

    def post_publish(self, topic):
        if self._tracer:
            self._tracer.active_span.finish()

    def pre_process_message(self, subscription, message):
        trace_parent = {
            ELASTIC_APM_TRACE_PARENT: message.attributes.get(ELASTIC_APM_TRACE_PARENT)
        }
        parent_span_context = self._tracer.extract(
            Format.TEXT_MAP,
            trace_parent
        )

        span_context = self._tracer.start_active_span(
            str(subscription),
            child_of=parent_span_context,
            finish_on_close=False
        )

        for key, value in message.attributes.items():
            span_context.span.set_tag(key, value)

    def post_process_message(self):
        self._tracer.active_span.finish()
