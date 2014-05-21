import pytest

def pytest_addoption(parser):
    parser.addoption('--domain', action='store', default='mydomain2',
                     help='Domain to use for integration tests')
    parser.addoption('--task-list', action='store', default='tasklist',
                     help='Task list name for integration tests')
    parser.addoption('--region', action='store', default='us-east-1',
                     help='Region for integration tests')

@pytest.fixture(scope='session')
def integration_test_args(request):
    return dict(
        domain=request.config.getoption('domain'),
        tasklist=request.config.getoption('task_list'),
        region=request.config.getoption('region'),
        )


