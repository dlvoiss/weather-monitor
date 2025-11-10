import gb

#####################
# Weather Defines
#####################

# Weather message types
AVG_SUNTIMES         = 0
AVG_TEMPF            = 1
AVG_INIT             = 2
AVG_DAY_HIGH_LOW     = 3
AVG_DAY_HIGH_LOW_GET = 4

#####################
# Weather Functions
#####################

def get_avg_msg_str(avg_msgType):
    switcher={
        AVG_SUNTIMES:'AVG_SUNTIMES',
        AVG_TEMPF:'AVG_TEMPF',
        AVG_INIT:'AVG_INIT',
        AVG_DAY_HIGH_LOW:'AVG_DAY_HIGH_LOW',
        AVG_DAY_HIGH_LOW_GET:'AVG_DAY_HIGH_LOW_GET',
    }
    return switcher.get(avg_msgType, "AVG MSG TYPE INVALID")
