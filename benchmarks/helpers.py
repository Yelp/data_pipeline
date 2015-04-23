import locale
import time


locale.setlocale(locale.LC_ALL, 'en_US')


def iter_timing(iterations=100000):
    start = time.time()
    for i in xrange(iterations):
        yield
    end = time.time()
    total_seconds = end - start
    pretty_iters = locale.format("%d", iterations, grouping=True)
    rate = locale.format("%.2f", round(iterations / total_seconds, 2), grouping=True)
    print "Total Time (%s iters): %s seconds" % (pretty_iters, total_seconds)
    print "Rate: %s/second" % rate
