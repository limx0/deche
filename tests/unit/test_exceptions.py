import time

import pytest

from deche import Cache


@pytest.fixture
def memory_cache():
    return Cache(fs_protocol="memory", prefix="/")


def test_exception_not_cached(memory_cache):
    @memory_cache
    def failing_func():
        time.sleep(0.1)
        raise ValueError("This function always fails")

    # First call should raise the exception
    start_time = time.time()
    with pytest.raises(ValueError):
        failing_func()
    first_call_time = time.time() - start_time

    # Second call should also raise the exception, not return a cached exception
    start_time = time.time()
    with pytest.raises(ValueError):
        failing_func()
    second_call_time = time.time() - start_time

    assert failing_func.list_cached_data() == []
    assert abs(first_call_time - second_call_time) < 0.05  # Both calls should take similar time


def test_successful_execution_after_exception(memory_cache):
    call_count = 0

    @memory_cache
    def sometimes_failing_func(fail=True):
        nonlocal call_count
        call_count += 1
        time.sleep(0.1)
        if fail:
            raise ValueError("This function fails when fail=True")
        return "Success"

    # First call should raise the exception
    start_time = time.time()
    with pytest.raises(ValueError):
        sometimes_failing_func(fail=True)
    exception_time = time.time() - start_time

    # Second call with fail=False should execute the function and cache the result
    start_time = time.time()
    result = sometimes_failing_func(fail=False)
    success_time = time.time() - start_time

    assert result == "Success"
    assert call_count == 2
    assert exception_time > 0.1
    assert success_time > 0.1

    # Third call with fail=False should return the cached result
    start_time = time.time()
    result = sometimes_failing_func(fail=False)
    cached_time = time.time() - start_time

    assert result == "Success"
    assert call_count == 2  # Call count shouldn't increase
    assert cached_time < 0.01  # Cached call should be very fast


def test_cache_behavior_unchanged_for_successful_calls(memory_cache):
    call_count = 0

    @memory_cache
    def cached_func(x):
        nonlocal call_count
        call_count += 1
        time.sleep(0.1)
        return x * 2

    # First call should execute the function
    start_time = time.time()
    result = cached_func(5)
    first_call_time = time.time() - start_time

    assert result == 10
    assert call_count == 1
    assert first_call_time > 0.1

    # Second call should return cached result
    start_time = time.time()
    result = cached_func(5)
    second_call_time = time.time() - start_time

    assert result == 10
    assert call_count == 1  # Call count shouldn't increase
    assert second_call_time < 0.01  # Cached call should be very fast

    # Call with different argument should execute the function again
    start_time = time.time()
    result = cached_func(7)
    third_call_time = time.time() - start_time

    assert result == 14
    assert call_count == 2
    assert third_call_time > 0.1
