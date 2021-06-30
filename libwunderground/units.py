# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

class Units:
    """
    https://weather.com/swagger-docs/call-for-code
    https://weather.com/swagger-docs/ui/sun/v2/sunV2PWSObservationsCurrentConditions.json
    """
    METRIC = 'm'
    IMPERIAL = 'e'
    HYBRID = 'h'

    UNITS = [METRIC, IMPERIAL, HYBRID]
