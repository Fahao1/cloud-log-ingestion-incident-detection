from scripts.load_test import percentile


def test_p95_uses_nearest_rank():
    assert percentile(list(range(1, 101)), 0.95) == 95
    assert percentile([12], 0.95) == 12
