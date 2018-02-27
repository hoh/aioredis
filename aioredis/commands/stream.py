from itertools import chain


class StreamCommandsMixin:
    """Stream commands mixin.

    For commands details see: http://redis.io/commands/#pubsub
    """

    def xadd(self, stream, values, id='*', maxlen=None, approximate=True):
        """Add values to a stream"""
        # Flatten the dict into a one-dimension iterator
        args = chain(*values.items())
        if maxlen:
            option = ('MAXLEN ~' if approximate else 'MAXLEN', maxlen)
        else:
            option = tuple()

        return self.execute(b'XADD', stream, *option, id, *args)

    def xrange(self, stream, start='-', stop='+', count=None):
        """Read stream values within an interval."""
        option = ('COUNT', count) if count else tuple()
        return self.execute(b'XRANGE', stream, start, stop, *option)

    def xrevrange(self, stream, start='+', stop='-', count=None):
        """Read stream values within an interval, in reverse order."""
        option = ('COUNT', count) if count else tuple()
        return self.execute(b'XREVRANGE', stream, start, stop, *option)

    def xread(self, stream, start='$', block=None, count=None):
        """Read a stream value, or wait for n seconds until a value
        is available."""
        block_option = ('BLOCK', block) if block else tuple()
        count_option = ('COUNT', count) if count else tuple()

        return self.execute(b'XREAD', *block_option, *count_option,
                            'STREAMS', stream, start)

    def xlen(self, stream):
        return self.execute(b'XRANGE', stream)
