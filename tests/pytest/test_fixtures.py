def test_hello(testdir):
    """Make sure that our plugin works."""

    # create a temporary pytest test file
    testdir.makepyfile(
        """
        import rele
        def test_publish(mock_rele_publish):
            rele.publish(topic='foobar', data={'baz': 1})
            mock_rele_publish.assert_called_once()

    """
    )

    # run all tests with pytest
    result = testdir.runpytest()

    # check that all 4 tests passed
    result.assert_outcomes(passed=1)
