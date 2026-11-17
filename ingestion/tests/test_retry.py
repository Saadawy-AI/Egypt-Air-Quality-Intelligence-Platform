from ingestion.utils.retry import retry

attempts = 0


@retry(max_retries=3, backoff_factor=0.1)
def unstable_function():
    global attempts
    attempts += 1

    if attempts < 3:
        raise ConnectionError("Simulated connection failure")

    return "Success"


def test_retry():
    result = unstable_function()

    assert result == "Success"
    assert attempts == 3

    print("Retry mechanism test passed!")
