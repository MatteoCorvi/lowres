import pytest
from lowres import products


class TestProduct_1(products.SatelliteProduct):
    PROD_ID = 'TEST_PROD_ID_1'

class TestProduct_2(products.SatelliteProduct):
    PROD_ID = 'TEST_PROD_ID_2'

class TestProduct_2_2(products.SatelliteProduct):
    PROD_ID = 'TEST_PROD_ID_2_2'


# Fixture to provide test products
@pytest.fixture
def test_products():
    return [TestProduct_1, TestProduct_2, TestProduct_2_2]


def test_available_products(test_products):
    assert products._available_products(__name__) == test_products


# Parametrize case sensitivity tests
@pytest.mark.parametrize("search_term", [
    'TEST_PROD_ID_1',
    'test_prod_id_1', 
    'Test_Prod_ID_1',
])
def test_match_products_case_insensitive(search_term, test_products):
    assert products.match_products(search_term, module_name=__name__) == [test_products[0]]


def test_match_products_when_similar(test_products):
    assert products.match_products('TEST_PROD_ID_2', module_name=__name__) == [test_products[1]]


# Parametrize wildcard pattern tests
@pytest.mark.parametrize("pattern,select", [
    ('Test*1', slice(0, 1)),
    ('Test*2', slice(1, 3)),
    ('Test*', slice(None)),
    ('*', slice(None)),
])
def test_match_products_wildcards(test_products, pattern, select):
    assert products.match_products(pattern, module_name=__name__) == test_products[select]


# Parametrize list input tests
@pytest.mark.parametrize("patterns,select", [
    (['Test*1', 'Test*2'], slice(None)),
    (['T&st'], slice(0)),
])
def test_match_products_list_inputs(test_products, patterns, select):
    assert products.match_products(patterns, module_name=__name__) == test_products[select]


# Test error cases
@pytest.mark.parametrize("invalid_input,expected_error", [
    (None, TypeError),
    ([], ValueError),
    ('', ValueError),
    ([''], ValueError), 
])
def test_match_products_error_cases(invalid_input, expected_error):
    with pytest.raises(expected_error):
        products.match_products(invalid_input, module_name=__name__)


# Optional: skip or mark tests based on conditions
#@pytest.mark.skipif(
#    not hasattr(products, 'some_optional_feature'),
#    reason="Optional feature not available"
#)

#def test_optional_feature():
#    pass


## Optional: add slow tests marker
#@pytest.mark.slow
#def test_performance():
#    # Add performance test here
#    pass


# Optional: add custom marker for specific test category
#pytest.mark.product_matching = pytest.mark.define("Tests for product matching functionality")
