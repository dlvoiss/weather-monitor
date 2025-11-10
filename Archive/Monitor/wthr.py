import gb
import db

#####################
# Weather Defines
#####################

# Weather message types
WTHR_INIT_COMPLETE      = 0
WTHR_INIT_CUR_MO        = 1
WTHR_INIT_ALL_TIME      = 2


MAX_DFLT_HUMID = 110.0
MIN_DFLT_HUMID = 0.0

MAX_DFLT_MB = 2000.0
MIN_DFLT_MB = 0.0

MAX_DFLT_TEMP = gb.PRIOR_TEMP_DFLT
MIN_DFLT_TEMP = 130.0

#####################
# Weather Functions
#####################

def get_weather_msg_str(wthr_msgType):
    switcher={
        WTHR_INIT_COMPLETE:'WTHR_INIT_COMPLETE',
        WTHR_INIT_CUR_MO:'WTHR_INIT_CUR_MO',
        WTHR_INIT_ALL_TIME:'WTHR_INIT_ALL_TIME',
    }
    return switcher.get(wthr_msgType, "WEATHER MSG TYPE INVALID")
