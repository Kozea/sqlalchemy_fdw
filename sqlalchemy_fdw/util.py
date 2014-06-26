"""Utilities used by both the dialect and the schema objects"""


def sql_options(options, preparer):
    """Format an options clause, if any"""
    if options:
        return ' options (%s)' % ','.join([
            "%s '%s'" % (preparer.quote_identifier(key), value)
            for key, value in list(options.items())
        ])
    return ''
