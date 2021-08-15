# location of this file indicates root for pytest
import os

#
import pytest, ray


@pytest.fixture(scope="session", autouse=True)
def init_ray_before_all_tests():
    # - Init ray once before all tests.
    # - This way there is no need for ray.init commands in the in test python files.
    #   Hence ray do not need to start when VS Code is discovering tests (which seems
    #   to generate errors).
    ray.init(num_cpus=2)


def repeat_in_stress_tests(f):
    """
    Some tests currently fail randomly

    With this decorator we can mark tests so that they
    - run repeatedly when RUN_ENVIRONMENT=stress-tests (to better catch if they fail randomly)
    - run once in other environments

    For other strategies: https://docs.pytest.org/en/6.2.x/flaky.html
    """
    if os.environ.get("RUN_ENVIRONMENT", "") == "stress-tests":
        repeat_count = 50
    else:
        repeat_count = 5

    return pytest.mark.parametrize("repeat_count", range(repeat_count))(f)
