from rele.contrib.apm_middleware import Carrier, ELASTIC_APM_TRACE_PARENT


class TestCarrier:
    def test_is_empty_on_initialization(self):
        carrier = Carrier()

        assert not carrier

    def test_is_a_dict(self):
        assert isinstance(Carrier(), dict)

    def test_can_be_populated(self):
        carrier = Carrier()
        carrier['key'] = 'value'

        assert carrier['key'] == 'value'

    def test_trace_parent_is_retrieved_as_string(self):
        carrier = Carrier()

        carrier[ELASTIC_APM_TRACE_PARENT] = b'1234'

        assert carrier.get_trace_parent() == '1234'
