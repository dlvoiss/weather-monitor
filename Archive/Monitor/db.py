import gb

#####################
# Database Defines
#####################

# DB message types
DB_CPU_FAN               = 0
DB_CPU_TEMPERATURE       = 1
DB_LOCAL_STATS           = 2
DB_REMOTE_STATS          = 3
DB_CUR_STATS             = 4
DB_INIT_READINGS         = 5
DB_AVG_TEMP              = 6
DB_INIT_AVG_TEMP         = 7
DB_INIT_CUR_MO           = 8
DB_INIT_ALL_TIME         = 9
DB_UPDATE_MO_DATA        = 10
DB_AVG_HI_LO_RESET       = 11
DB_UPDATE_AVG_HI_LO      = 12
DB_AVG_HI_LO_GET         = 13
DB_READING               = 14
DB_GUST                  = 15
DB_MAX_1_HOUR            = 16
DB_MAX_TODAY             = 17
DB_SUNTIMES              = 18

DB_TEST                  = 29
DB_EXIT                  = 30   # Keep this msg type LAST (highest integer)


# SQL field identifies for SQL_UPDT_CUR_MO and SQL_UPDT_ALL_TIME
DB_MIN = 'min'
DB_MAX = 'max'
DB_TEMPF = 'tempf'
DB_HUM = 'hum'
DB_MB = 'mB'
DB_CUR_MO = 0
DB_ALL_TIME = 1

#####################
# Database Functions
#####################

def get_db_msg_str(db_msgType):
    switcher={
        DB_CPU_FAN:'DB_CPU_FAN',
        DB_CPU_TEMPERATURE:'DB_CPU_TEMPERATURE',
        DB_LOCAL_STATS:'DB_LOCAL_STATS',
        DB_REMOTE_STATS:'DB_REMOTE_STATS',
        DB_CUR_STATS:'DB_CUR_STATS',
        DB_INIT_READINGS:'DB_INIT_READINGS',
        DB_AVG_TEMP:'DB_AVG_TEMP',
        DB_INIT_AVG_TEMP:'DB_INIT_AVG_TEMP',
        DB_INIT_CUR_MO:'DB_INIT_CUR_MO',
        DB_INIT_ALL_TIME:'DB_INIT_ALL_TIME',
        DB_UPDATE_MO_DATA:'DB_UPDATE_MO_DATA',
        DB_AVG_HI_LO_RESET:'DB_AVG_HI_LO_RESET',
        DB_UPDATE_AVG_HI_LO:'DB_UPDATE_AVG_HI_LO',
        DB_AVG_HI_LO_GET:'DB_AVG_HI_LO_GET',
        DB_READING:'DB_READING',
        DB_GUST:'DB_GUST',
        DB_MAX_1_HOUR:'DB_MAX_1_HOUR',
        DB_MAX_TODAY:'DB_MAX_TODAY',
        DB_SUNTIMES:'DB_SUNTIMES',
        DB_TEST:'DB_TEST',
        DB_EXIT:'DB_EXIT',
    }
    return switcher.get(db_msgType, "DB MSG TYPE INVALID")

