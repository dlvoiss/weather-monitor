import gb
import db
import wthr
import avg
import requests
import json
import re
import glob
from board import SCL, SDA
import busio
from bmp280 import BMP280
from smbus import SMBus
import board
import adafruit_dht

# END_EVENT_CHECK controls main loop sleep time (seconds) for weather monitoring
END_EVENT_CHECK = 10

LOCAL_FREQUENCY = 600 / END_EVENT_CHECK  # 10 minutes or 600 seconds
#LOCAL_FREQUENCY = 3  # Debugging value
DHT_SLEEP_TIME = 4

#REMOTE_FREQUENCY = (3600 / END_EVENT_CHECK) * 1  # 1 hours or 3600 seconds
REMOTE_FREQUENCY = (3600 / END_EVENT_CHECK) * 3  # 3 hours or 10800 seconds

DHT_PIN = board.D25

MAX_DFLT_F = gb.PRIOR_TEMP_DFLT
MIN_DFLT_F = 212.0

SECS_IN_A_DAY = 86400.0

DHT_SENSOR = 0
BMP_SENSOR = 1

MAX_DATE = gb.DFLT_TIME
MIN_DATE = gb.DFLT_TIME

MAX_DHT_F = MAX_DFLT_F
MIN_DHT_F = MIN_DFLT_F
MAX_DHT_TS = gb.DFLT_TIME
MIN_DHT_TS = gb.DFLT_TIME

MAX_BMP_F = MAX_DFLT_F
MIN_BMP_F = MIN_DFLT_F
MAX_BMP_TS = gb.DFLT_TIME
MIN_BMP_TS = gb.DFLT_TIME

MAX_HUMID = wthr.MIN_DFLT_HUMID
MIN_HUMID = wthr.MAX_DFLT_HUMID
MAX_HUMID_TS = gb.DFLT_TIME
MIN_HUMID_TS = gb.DFLT_TIME

MAX_MB = wthr.MIN_DFLT_MB
MIN_MB = wthr.MAX_DFLT_MB
MAX_MB_TS = gb.DFLT_TIME
MIN_MB_TS = gb.DFLT_TIME

MAX_30_DHT_F = MAX_DFLT_F
MIN_30_DHT_F = MIN_DFLT_F
MAX_30_DHT_TS = gb.DFLT_TIME
MIN_30_DHT_TS = gb.DFLT_TIME

MAX_30_BMP_F = MAX_DFLT_F
MIN_30_BMP_F = MIN_DFLT_F
MAX_30_BMP_TS = gb.DFLT_TIME
MIN_30_BMP_TS = gb.DFLT_TIME

MAX_30_HUMID = wthr.MIN_DFLT_HUMID
MIN_30_HUMID = wthr.MAX_DFLT_HUMID
MAX_30_HUMID_TS = gb.DFLT_TIME
MIN_30_HUMID_TS = gb.DFLT_TIME

MAX_30_MB = wthr.MAX_DFLT_MB
MIN_30_MB = wthr.MIN_DFLT_MB
MAX_30_MB_TS = gb.DFLT_TIME
MIN_30_MB_TS = gb.DFLT_TIME

#######################################################
#
# Remote weather defines
#
#######################################################
# API KEY
API_key = "e51500697bfb17ea5f215eba6c0576ca"
MOUNTAIN_VIEW_CA = "6079660"
MOUNTAIN_VIEW_CA_ZIP = "94040"

# Pa is a Pascal, and is one newton per square meter
# hPa is hectopascal, and is 100 Pascals, and also equal to 1 millibar
# kPa is kilopascal, and is 1000 Pascals

# P-hPa - P-kPa converions
#  P-hPa = P-kPa / 10
#  P-kPa = 10 * P-hPa
# P-kPa - P-mb converions
#   P-mb = 10 * P-kPa
#   P-kPa = P-mb / 10
# P-kPa - P-atm converions
#   P-atm = 0.009869233 * P-kPa
#   P-kPa = 101.3250 * P-atm
# P-kPa - P-psi converions
#   P-psi = 0.145038 * P-kPa
#   P-kPa = 6.89476 * P-psi
# P-kPa - P-mmHg converions
#   P-mmHg = 7.50062 * P-kPa
#   P-kPa = 0.1333224 * P-mmHg
# P-kPa - P-inHg converions
#   P-inHg = 0.295300 * P-kPa
#   P-inHg = 33.8638 * P-hPa
#   P-kPa = 33.8639 * P-inHg / 10

#  P-mb = P-hPa
#  P-inHg = 0.0295300 * P-mb

# Sea level pressure in mmHg:          760 mmHg
# Sea level pressure in inHg:       29.921 inHg
# Sea level pressure in millibar: 1013.325 mb
# Sea level pressure in hPa:      1013.325 hPa
# Sea level pressure in kPa:      101.3325 kPa
# Sea level pressure in psi:        14.696 lb / in^2

# Pressure change of 1hPa = 8.43m at sea level
# 1 foot of air equals 0.038640888 hPa

# Mountain View, CA via api.openweathermap.org is McKelvey Park location
# with altitude (from Google Earth) of 103 feet.  Our house on Columbia Dr.
# has altitude 136 feet (also from Google Earth.)
# 136 ft = 41.4528 meters
# 103 ft = 31.3944 meters

# So Columbia Dr. base pressure is about 4.0173 hPa (41.4528 m / 8.43) less
# than sea level pressure.   So Columbia Dr. base pressure would
# be 1013.325 - 4.0173 = 1009.30 hPa or 1009.30 mb

# Standard sea level pressure in hPa
SEA_LEVEL_PRESSURE = 1013.325
# Base Columbia Dr. pressure in hPa
# Columbia Dr. altitude (from Google maps)
COLUMBIA_ALTITUDE_M = 41.4528
COLUMBIA_hPa_ADJUST = -4.0173
COLUMBIA_ALTITUDE_FT = 136
hPa_per_meter = 0.12677457
hPa_per_foot = 0.038640888

#######################################################################
#
# Weather Thread
# Reads local and remote weather sensors
#
#######################################################################
class WeatherThread(gb.threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        gb.threading.Thread.__init__(self, group=group, target=target, name=name)
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.kill_received = False

        self.need_current_month = True
        self.current_month = 0
        self.need_all_time_month = True
        self.all_time_month = 0

        self.sunrise_str = ""
        self.sunset_str = ""

        self.min_temp_cur_mo = wthr.MIN_DFLT_TEMP
        self.min_temp_cur_mo_ts = gb.DFLT_TIME
        self.max_temp_cur_mo = wthr.MAX_DFLT_TEMP
        self.max_temp_cur_mo_ts = gb.DFLT_TIME

        self.min_humidity_cur_mo = wthr.MAX_DFLT_HUMID
        self.min_humidity_cur_mo_ts = gb.DFLT_TIME
        self.max_humidity_cur_mo = wthr.MIN_DFLT_HUMID
        self.max_humidity_cur_mo_ts = gb.DFLT_TIME

        self.min_pressure_cur_mo = wthr.MAX_DFLT_MB
        self.min_pressure_cur_mo_ts = gb.DFLT_TIME
        self.max_pressure_cur_mo = wthr.MIN_DFLT_MB
        self.max_pressure_cur_mo_ts = gb.DFLT_TIME

        self.min_temp_all_time = wthr.MIN_DFLT_TEMP
        self.min_temp_all_time_ts = gb.DFLT_TIME
        self.max_temp_all_time = wthr.MAX_DFLT_TEMP
        self.max_temp_all_time_ts = gb.DFLT_TIME

        self.min_humidity_all_time = wthr.MAX_DFLT_HUMID
        self.min_humidity_all_time_ts = gb.DFLT_TIME
        self.max_humidity_all_time = wthr.MIN_DFLT_HUMID
        self.max_humidity_all_time_ts = gb.DFLT_TIME

        self.min_pressure_all_time = wthr.MAX_DFLT_MB
        self.min_pressure_all_time_ts = gb.DFLT_TIME
        self.max_pressure_all_time = wthr.MIN_DFLT_MB
        self.max_pressure_all_time_ts = gb.DFLT_TIME

        return

    def get_kPa(self, hPa):
        kPa = hPa / 10
        return kPa

    def get_Pa(self, hPa):
        Pa = hPa * 100
        return Pa

    def get_inHg(self, hPa):
        inHg = 0.0295300 * hPa
        return inHg

    def get_mmHg(self, hPa):
        mmHg = (7.50062 * hPa)/10
        return mmHg

    def get_psi(self, hPa):
        psi = 0.0145038 * hPa
        return psi

    def get_atm(self, hPa, local_pressure):
        atm = hPa / local_pressure
        return atm

    #----------------------------------
    # pressure reading in hPa, altitude in meters
    #----------------------------------
    def get_adjusted_sea_level(self, hPa, alt):
        hPa_adjust = alt * hPa_per_meter
        gb.logging.debug("Meters: %.2f, hPa Adjustment: %.2f" % (alt, hPa_adjust))
        sea_level = hPa + hPa_adjust
        return(sea_level)

    #----------------------------------
    # pressure reading in hPa, altitude in feet
    #----------------------------------
    def get_ft_adjusted_sea_level(self, hPa, alt):
        hPa_adjust = alt * hPa_per_foot
        gb.logging.debug("Feet: %.2f, hPa Adjustment: %.2f" % (alt, hPa_adjust))
        sea_level = hPa + hPa_adjust
        return(sea_level)

    #----------------------------------
    # hPa in x feet
    #----------------------------------
    def get_hpa_from_feet(self, feet):
        hPa_adjust = feet * hPa_per_foot
        return(hPa_adjust)

    #----------------------------------
    # Get number of days in timedelta parameter
    #----------------------------------
    def get_days_in_delta(self, delta):
        secs = delta.total_seconds()
        return(secs / SECS_IN_A_DAY)

    #----------------------------------
    # Get maximum reading today
    #----------------------------------
    def get_max_today_reading(self, cur_tm, cur_reading, max_tm,
                              max_reading, cur_day):

        global MAX_DATE
        max_val = gb.NO_CHANGE

        # Add 0.5 degrees for sensor sensitivity (false precision)
        if (cur_reading > (max_reading + 0.5)):
            max_val = cur_reading
            MAX_DATE = cur_tm
            gb.logging.debug("max today >")
        else:
            # cur_day is set to 00:00:00 on current day
            # if date for maximum reading is not today (i.e., prior to
            # 00:00:00 on current day), update max reading
            if (max_tm < cur_day):
                max_val = cur_reading
                MAX_DATE = cur_tm
                gb.logging.debug("max date today")

            # else no change to maximum reading

        return(max_val)

    #----------------------------------
    # Get minimum reading today
    #----------------------------------
    def get_min_today_reading(self, cur_tm, cur_reading, min_tm,
                              min_reading, cur_day):

        global MIN_DATE
        min_val = gb.NO_CHANGE

        # Add 0.5 degrees for sensor sensitivity (false precision)
        if (cur_reading < (min_reading - 0.5)):
            min_val = cur_reading
            MIN_DATE = cur_tm
            gb.logging.debug("min today >")
        else:
            # cur_day is set to 00:00:00 on current day
            # if date for minimum reading is not today (i.e., prior to
            # 00:00:00 on current day), update max reading
            if (min_tm < cur_day):
                min_val = cur_reading
                MIN_DATE = cur_tm
                gb.logging.debug("min date today")

            # else no change to minimum reading

        return(min_val)

    #----------------------------------
    # Get maximum reading in prior x days
    # Reading must be float number (e.g., 10.1, etc.)
    #----------------------------------
    def get_max_reading(self, cur_tm, cur_reading, max_tm,
                        max_reading, num_days):

        global MAX_DATE
        max_val = gb.NO_CHANGE

        # Add 0.5 degrees for sensor sensitivity (false precision)
        if (cur_reading > (max_reading + 0.5)):
            max_val = cur_reading
            MAX_DATE = cur_tm
            gb.logging.debug("max >")
        else:
            # if date for maximum is more than num_days old, update max reading
            delta = cur_tm - max_tm
            delta_days = self.get_days_in_delta(delta)
            if (delta_days >= num_days):
                max_val = cur_reading
                MAX_DATE = cur_tm
                gb.logging.debug("max date")

            # else no change to maximum reading

        return(max_val)

    #----------------------------------
    # Get minimum reading in prior x days
    # Reading must be float number (e.g., 10.1, etc.)
    #----------------------------------
    def get_min_reading(self, cur_tm, cur_reading, min_tm,
                        min_reading, num_days):

        global MIN_DATE
        min_val = gb.NO_CHANGE

        # Subtract 0.5 degrees for sensor sensitivity (false precision)
        if (cur_reading < (min_reading - 0.5)):
            min_val = cur_reading
            MIN_DATE = cur_tm
            gb.logging.debug("min <")
        else:
            # if date for minimum is more than num_days old, update min reading
            delta = cur_tm - min_tm
            delta_days = self.get_days_in_delta(delta)
            if (delta_days >= num_days):
                min_val = cur_reading
                MIN_DATE = cur_tm
                gb.logging.debug("min date")

            # else no change to minimum reading

        return(min_val)

    #----------------------------------
    # Get minimum of two readings reading
    #----------------------------------
    def init_cur_mo_data(self, weather_data):

        current_month = weather_data[1]
        current_month_str = weather_data[2]

        if (current_month != self.current_month):
            gb.logging.error("DB month(%d) unequal to self.current_month(%d)" %
                             (current_month, self.current_month))

        self.min_temp_cur_mo_ts = weather_data[3]
        self.min_temp_cur_mo = float(weather_data[4])
        self.max_temp_cur_mo_ts =  weather_data[5]
        self.max_temp_cur_mo = float(weather_data[6])

        self.min_humidity_cur_mo_ts = weather_data[7]
        self.min_humidity_cur_mo = float(weather_data[8])
        self.max_humidity_cur_mo_ts = weather_data[9]
        self.max_humidity_cur_mo = float(weather_data[10])

        self.min_pressure_cur_mo_ts = weather_data[11]
        self.min_pressure_cur_mo = float(weather_data[12])
        self.max_pressure_cur_mo_ts = weather_data[13]
        self.max_pressure_cur_mo = float(weather_data[14])

        # Let waiting loop know data is initialized
        self.need_current_month = False

        if (gb.DIAG_LEVEL & 0x2000):
            gb.logging.info("need_current_month set to %s for %s(%d)" %
                            (self.need_current_month, current_month_str,
                             current_month))
        gb.logging.info("Current month data initialized for %s from DB" %
                        (current_month_str))

    #----------------------------------
    # Get minimum of two readings reading
    #----------------------------------
    def init_all_time_data(self, weather_data):

        current_month = weather_data[1]
        current_month_str = weather_data[2]

        if (current_month != self.all_time_month):
            gb.logging.error("DB month(%d) unequal to self.all_time_month(%d)" %
                             (current_month, self.all_time_month))

        self.min_temp_all_time_ts = weather_data[3]
        self.min_temp_all_time = float(weather_data[4])
        self.max_temp_all_time_ts =  weather_data[5]
        self.max_temp_all_time = float(weather_data[6])

        self.min_humidity_all_time_ts = weather_data[7]
        self.min_humidity_all_time = float(weather_data[8])
        self.max_humidity_all_time_ts = weather_data[9]
        self.max_humidity_all_time = float(weather_data[10])

        self.min_pressure_all_time_ts = weather_data[11]
        self.min_pressure_all_time = float(weather_data[12])
        self.max_pressure_all_time_ts = weather_data[13]
        self.max_pressure_all_time = float(weather_data[14])

        gb.logging.info("Temperature: MIN: %.1f, MAX: %.1f" %
                  (self.min_temp_all_time, self.max_temp_all_time))
        gb.logging.info("Humidity: MIN: %.1f, MAX: %.1f" %
                  (self.min_humidity_all_time, self.max_humidity_all_time))
        gb.logging.info("Pressure: MIN: %.1f, MAX: %.1f" %
                  (self.min_pressure_all_time, self.max_pressure_all_time))

        # Let waiting loop know data is initialized
        self.need_all_time_month = False

        if (gb.DIAG_LEVEL & 0x2000):
            gb.logging.info("need_all_time_month set to %s for month %s(%d)" %
                            (self.need_all_time_month, current_month_str,
                             current_month))

        gb.logging.info("All-time month data initialized for %s from DB" %
                        (current_month_str))

    #----------------------------------
    # Get minimum of two readings reading
    #----------------------------------
    def get_min(self, cur_time, reading, min_reading):
        min_val = min_reading
        # Subtract 0.5 degrees for sensor sensitivity (false precision)
        if (reading < (min_reading - 0.5)):
            min_val = reading
            gb.logging.debug("min <")
        return(min_val)

    #----------------------------------
    # Get maximum of two readings reading
    #----------------------------------
    def get_max(self, cur_time, reading, max_reading):
        max_val = max_reading
        # Subtract 0.5 degrees for sensor sensitivity (false precision)
        if (reading > (max_reading + 0.5)):
            max_val = reading
            gb.logging.debug("max >")
        return(max_val)

    #----------------------------------
    # Check if current date is within current month
    # Update month if date not within current month
    #----------------------------------
    def check_cur_month(self, month, db_q_out):
        if (self.current_month != month):
            self.need_current_month = True
            self.current_month = month
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("need_current_month set to %s for month %d" %
                                (self.need_current_month, month))

            db_msgType = db.DB_INIT_CUR_MO
            dbInfo = []
            dbInfo.append(db_msgType)
            dbInfo.append(month)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d)" %
                     (db.get_db_msg_str(db_msgType),db_msgType))
            db_q_out.put(dbInfo)

        return self.need_current_month

    #----------------------------------
    # Check if current date is within current month
    # Update month if date not within current month
    # and request current month data from DB
    #----------------------------------
    def check_all_time(self, month, db_q_out):
        if (self.all_time_month != month):
            
            self.need_all_time_month = True
            self.all_time_month = month
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("need_all_time_month set to %s for month %d" %
                                (self.need_all_time_month, month))

            db_msgType = db.DB_INIT_ALL_TIME
            dbInfo = []
            dbInfo.append(db_msgType)
            dbInfo.append(month)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d)" %
                     (db.get_db_msg_str(db_msgType),db_msgType))
            db_q_out.put(dbInfo)

        return self.need_all_time_month

    #----------------------------------
    # Get minimum/maximum reading for current month
    #----------------------------------
    def get_cur_month_min_max(self, cur_time, dht_temp_f, pressure_hPa,
                              humidity, db_q_out, end_event):

        month = cur_time.month
        db_update_sent = False

        tm_str = gb.get_date_with_seconds(str(cur_time))

        # Check if current month data has been initialized;
        # if not, retrieve current month data from DB
        self.need_current_month = self.check_cur_month(month, db_q_out)

        if (self.need_current_month):
            # Don't update DB until after DB data for current month
            # data has been initialized
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Returning from get_cur_month_min_max()")
            return()

        if (gb.DIAG_LEVEL & 0x1000):
            gb.logging.info("Continuing with get_cur_month_min_max()")

        # Check minimum readings for the current month

        min_val = self.get_min(cur_time, dht_temp_f, self.min_temp_cur_mo)
        if (min_val != self.min_temp_cur_mo):
            self.min_temp_cur_mo = min_val
            self.min_temp_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month min temp updated: %s %.1f" %
                                (str(cur_time), min_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoTmin = []
            dbInfoTmin.append(db_msgType)
            dbInfoTmin.append(db.DB_MIN)
            dbInfoTmin.append(db.DB_TEMPF)
            dbInfoTmin.append(db.DB_CUR_MO)
            dbInfoTmin.append(month)
            dbInfoTmin.append(tm_str)
            dbInfoTmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoTmin[1],dbInfoTmin[2]))
            db_q_out.put(dbInfoTmin)
            db_update_sent = True

        min_val = self.get_min(cur_time, pressure_hPa, self.min_pressure_cur_mo)
        if (min_val != self.min_pressure_cur_mo):
            self.min_pressure_cur_mo = min_val
            self.min_pressure_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month min pressure updated: %s %.1f" %
                                (str(cur_time), min_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoPmin = []
            dbInfoPmin.append(db_msgType)
            dbInfoPmin.append(db.DB_MIN)
            dbInfoPmin.append(db.DB_MB)
            dbInfoPmin.append(db.DB_CUR_MO)
            dbInfoPmin.append(month)
            dbInfoPmin.append(tm_str)
            dbInfoPmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoPmin[1],dbInfoPmin[2]))
            db_q_out.put(dbInfoPmin)
            db_update_sent = True

        min_val = self.get_min(cur_time, humidity, self.min_humidity_cur_mo)
        if (min_val != self.min_humidity_cur_mo):
            self.min_humidity_cur_mo = min_val
            self.min_humidity_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month min humidity updated: %s %.1f" %
                                (str(cur_time), min_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoHmin = []
            dbInfoHmin.append(db_msgType)
            dbInfoHmin.append(db.DB_MIN)
            dbInfoHmin.append(db.DB_HUM)
            dbInfoHmin.append(db.DB_CUR_MO)
            dbInfoHmin.append(month)
            dbInfoHmin.append(tm_str)
            dbInfoHmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoHmin[1],dbInfoHmin[2]))
            db_q_out.put(dbInfoHmin)
            db_update_sent = True

        # Check maximum readings for the current month

        max_val = self.get_max(cur_time, dht_temp_f, self.max_temp_cur_mo)
        if (max_val != self.max_temp_cur_mo):
            self.max_temp_cur_mo = max_val
            self.max_temp_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month max temp updated: %s %.1f" %
                                (str(cur_time), max_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoTmax = []
            dbInfoTmax.append(db_msgType)
            dbInfoTmax.append(db.DB_MAX)
            dbInfoTmax.append(db.DB_TEMPF)
            dbInfoTmax.append(db.DB_CUR_MO)
            dbInfoTmax.append(month)
            dbInfoTmax.append(tm_str)
            dbInfoTmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoTmax[1],dbInfoTmax[2]))
            db_q_out.put(dbInfoTmax)
            db_update_sent = True

        max_val = self.get_max(cur_time, pressure_hPa, self.max_pressure_cur_mo)
        if (max_val != self.max_pressure_cur_mo):
            self.max_pressure_cur_mo = max_val
            self.max_pressure_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month max pressure updated: %s %.1f" %
                                (str(cur_time), max_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoPmax = []
            dbInfoPmax.append(db_msgType)
            dbInfoPmax.append(db.DB_MAX)
            dbInfoPmax.append(db.DB_MB)
            dbInfoPmax.append(db.DB_CUR_MO)
            dbInfoPmax.append(month)
            dbInfoPmax.append(tm_str)
            dbInfoPmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoPmax[1],dbInfoPmax[2]))
            db_q_out.put(dbInfoPmax)
            db_update_sent = True

        max_val = self.get_max(cur_time, humidity, self.max_humidity_cur_mo)
        if (max_val != self.max_humidity_cur_mo):
            self.max_humidity_cur_mo = max_val
            self.max_humidity_cur_mo_ts = cur_time
            if (gb.DIAG_LEVEL & 0x1000):
                gb.logging.info("Current month max humidity updated: %s %.1f" %
                                (str(cur_time), max_val))

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoHmax = []
            dbInfoHmax.append(db_msgType)
            dbInfoHmax.append(db.DB_MAX)
            dbInfoHmax.append(db.DB_HUM)
            dbInfoHmax.append(db.DB_CUR_MO)
            dbInfoHmax.append(month)
            dbInfoHmax.append(tm_str)
            dbInfoHmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoHmax[1],dbInfoHmax[2]))
            db_q_out.put(dbInfoHmax)
            db_update_sent = True

        if (db_update_sent):
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("UPDATED: %s" % (tm_str))
            # Give DB time to receive messages
            gb.time.sleep(2)

    #----------------------------------
    # Get all-time minimum/maximum reading for current month
    #----------------------------------
    def get_all_time_month_min_max(self, cur_time, dht_temp_f, pressure_hPa,
                                   humidity, db_q_out, end_event):

        month = cur_time.month
        db_update_sent = False

        tm_str = gb.get_date_with_seconds(str(cur_time))
        #tm_str = re.sub(' ', ",", tm_str)

        # Check if current month all-time data has been initialized;
        # if not, retrieve all-time data from DB

        self.need_all_time_month = self.check_all_time(month, db_q_out)

        if (self.need_all_time_month):

            # Don't update DB until after DB data for all-time month
            # data has been initialized (i.e., self.need_all_time_month
            # is False)

            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("Return from get_all_time_month_min_max()")
            return()

        if (gb.DIAG_LEVEL & 0x2000):
            gb.logging.info("Continuing with get_all_time_month_min_max()")

        # Check for new minimum all-time readings for current month

        min_val = self.get_min(cur_time, dht_temp_f, self.min_temp_all_time)
        if (min_val != self.min_temp_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("Pre: All time month min temp: %.1f" %
                                (self.min_temp_all_time))
                gb.logging.info("All time month min temp updated: %s %.1f" %
                                (str(cur_time), min_val))
            self.min_temp_all_time = min_val
            self.min_temp_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoTmin = []
            dbInfoTmin.append(db_msgType)
            dbInfoTmin.append(db.DB_MIN)
            dbInfoTmin.append(db.DB_TEMPF)
            dbInfoTmin.append(db.DB_ALL_TIME)
            dbInfoTmin.append(month)
            dbInfoTmin.append(tm_str)
            dbInfoTmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoTmin[1],dbInfoTmin[2]))
            db_q_out.put(dbInfoTmin)
            db_update_sent = True

        min_val = self.get_min(cur_time, pressure_hPa,
                               self.min_pressure_all_time)
        if (min_val != self.min_pressure_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("Pre: All time month min pressure: %.1f" %
                                (self.min_pressure_all_time))
                gb.logging.info("All time month min pressure updated: %s %.1f" %
                                (str(cur_time), min_val))
            self.min_pressure_all_time = min_val
            self.min_pressure_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoPmin = []
            dbInfoPmin.append(db_msgType)
            dbInfoPmin.append(db.DB_MIN)
            dbInfoPmin.append(db.DB_MB)
            dbInfoPmin.append(db.DB_ALL_TIME)
            dbInfoPmin.append(month)
            dbInfoPmin.append(tm_str)
            dbInfoPmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoPmin[1],dbInfoPmin[2]))
            db_q_out.put(dbInfoPmin)
            db_update_sent = True

        min_val = self.get_min(cur_time, humidity, self.min_humidity_all_time)
        if (min_val != self.min_humidity_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("Pre: All time month min humidity: %.1f" %
                                (self.min_humidity_all_time))
                gb.logging.info("All time month min humidity updated: %s %.1f" %
                                (str(cur_time), min_val))
            self.min_humidity_all_time = min_val
            self.min_humidity_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoHmin = []
            dbInfoHmin.append(db_msgType)
            dbInfoHmin.append(db.DB_MIN)
            dbInfoHmin.append(db.DB_HUM)
            dbInfoHmin.append(db.DB_ALL_TIME)
            dbInfoHmin.append(month)
            dbInfoHmin.append(tm_str)
            dbInfoHmin.append(min_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoHmin[1],dbInfoHmin[2]))
            db_q_out.put(dbInfoHmin)
            db_update_sent = True

        # Check for new maximum all-time readings for current month

        max_val = self.get_max(cur_time, dht_temp_f, self.max_temp_all_time)
        if (max_val != self.max_temp_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("All time month max temp updated: %s %.1f" %
                                (str(cur_time), max_val))
            self.max_temp_all_time = max_val
            self.max_temp_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoTmax = []
            dbInfoTmax.append(db_msgType)
            dbInfoTmax.append(db.DB_MAX)
            dbInfoTmax.append(db.DB_TEMPF)
            dbInfoTmax.append(db.DB_ALL_TIME)
            dbInfoTmax.append(month)
            dbInfoTmax.append(tm_str)
            dbInfoTmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoTmax[1],dbInfoTmax[2]))
            db_q_out.put(dbInfoTmax)
            db_update_sent = True

        max_val = self.get_max(cur_time, pressure_hPa,
                               self.max_pressure_all_time)
        if (max_val != self.max_pressure_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("All time month max pressure updated: %s %.1f" %
                                (str(cur_time), max_val))
            self.max_pressure_all_time = max_val
            self.max_pressure_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoPmax = []
            dbInfoPmax.append(db_msgType)
            dbInfoPmax.append(db.DB_MAX)
            dbInfoPmax.append(db.DB_MB)
            dbInfoPmax.append(db.DB_ALL_TIME)
            dbInfoPmax.append(month)
            dbInfoPmax.append(tm_str)
            dbInfoPmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoPmax[1],dbInfoPmax[2]))
            db_q_out.put(dbInfoPmax)
            db_update_sent = True

        max_val = self.get_max(cur_time, humidity, self.max_humidity_all_time)
        if (max_val != self.max_humidity_all_time):
            if (gb.DIAG_LEVEL & 0x2000):
                gb.logging.info("All time month max humidity updated: %s %.1f" %
                                (str(cur_time), max_val))
            self.max_humidity_all_time = max_val
            self.max_humidity_all_time_ts = cur_time

            db_msgType = db.DB_UPDATE_MO_DATA
            dbInfoHmax = []
            dbInfoHmax.append(db_msgType)
            dbInfoHmax.append(db.DB_MAX)
            dbInfoHmax.append(db.DB_HUM)
            dbInfoHmax.append(db.DB_ALL_TIME)
            dbInfoHmax.append(month)
            dbInfoHmax.append(tm_str)
            dbInfoHmax.append(max_val)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d) for %s %s" %
                     (db.get_db_msg_str(db_msgType),db_msgType,
                      dbInfoHmax[1],dbInfoHmax[2]))
            db_q_out.put(dbInfoHmax)
            db_update_sent = True

        if (db_update_sent):
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("UPDATED: %s" % (tm_str))
            # Give DB time to receive messages
            gb.time.sleep(2)

    #----------------------------------
    # Get online weather from remote server
    #----------------------------------
    def get_online_weather(self):
        # Mountain View, CA: 6079660
        # Sunnyvale, CA:     5400075
        # Palo Alto, CA:     5380748

        global API_key

        gb.logging.info("Requesting online weather - get_online_weather")

        # This stores the url
        base_url = "http://api.openweathermap.org/data/2.5/weather?"
        city_id = MOUNTAIN_VIEW_CA
        zip = MOUNTAIN_VIEW_CA_ZIP

        # This is final url. This is concatenation of base_url, API_key and city_id
        Final_url = base_url + "appid=" + API_key + "&zip=" + zip + "&units=imperial"

        gb.logging.debug(Final_url)

        # this variable contain the JSON data which the API returns
        try:
            weather_data = requests.get(Final_url).json()
        except simplejson.errors.JSONDecodeError as e:
            gb.logging.error("Could not decode json response from server")
            gb.logging.error("Error: %s" % (str(e)))
        except RuntimeError as error:
            gb.logging.error("Weather data request error: %s" % (error.args[0]))

        # Convert json object to string
        weather_data_str = json.dumps(weather_data)
        sp_weather = weather_data_str.split(',')

        gb.logging.info("Received online weather")

        return(sp_weather)

    #----------------------------------
    # Init last nday fields in currentreadings DB table
    #----------------------------------
    def get_combined_temperature(self, bmp, dht):

        BMP_SENSITIVITY = 1.8
        DHT_SENSITIVITY = 0.9
        midpoint = 0.0

        dhtinrange = False
        bmpinrange = False

        bmplow = bmp - BMP_SENSITIVITY
        bmphigh = bmp + BMP_SENSITIVITY
        dhtlow = dht - DHT_SENSITIVITY
        dhthigh = dht + DHT_SENSITIVITY

        if (gb.DIAG_LEVEL & 0x200):
            gb.logging.info("BMP: %.1f - %.1f" % (bmplow, bmphigh))
            gb.logging.info("DHT: %.1f - %.1f" % (dhtlow, dhthigh))

        if (dht == bmp):
            mid_point = dht
            if (gb.DIAG_LEVEL & 0x200):
                gb.logging.info("Readings are EQUAL")
        else:
            # if either reading falls within range of other sensor,
            # split the difference between the readings

            if (dht > bmplow and dht < bmphigh):
                dhtinrange = True
                if (gb.DIAG_LEVEL & 0x200):
                    gb.logging.info("DHT reading within BMP range")
            if (bmp > dhtlow and bmp < dhthigh):
                if (gb.DIAG_LEVEL & 0x200):
                    gb.logging.info("BMP reading within DHT range")
                bmpinrange = True

            if (bmpinrange or dhtinrange):
                diff = abs(dht - bmp)
                diff = diff/2.0
                if (bmp > dht):
                    midpoint = dht + diff
                else:
                    midpoint = bmp + diff

            else:
                # Check if ranges overlap, if so, split the difference
                # between the high low and the low high 

                if (dhtlow < bmphigh and bmphigh < dhthigh):
                    diff = abs(bmphigh - dhtlow)
                    diff = diff / 2.0
                    midpoint = dhtlow + diff
                    if (gb.DIAG_LEVEL & 0x200):
                        gb.logging.info("RANGES overlap, with BMP reading lower")
                elif (bmplow <= dhthigh and dhthigh <= bmphigh):
                    diff = abs(bmphigh - dhtlow)
                    diff = diff / 2.0
                    midpoint = bmplow + diff
                    if (gb.DIAG_LEVEL & 0x200):
                        gb.logging.info("RANGES overlap, with DHT reading lower")

                else:
                    # Ranges do not overlap, split the difference between
                    # the highest low range and the lowest high range
                    if (dhthigh > bmphigh):
                        diff = abs(dhtlow - bmphigh)
                        diff = diff / 2.0
                        midpoint = bmphigh + diff
                        if (gb.DIAG_LEVEL & 0x200):
                            gb.logging.info("RANGES do not overlap, with BMP reading lower")
                    else: # bmphigh > dhthigh
                        diff = abs(bmplow - dhthigh)
                        diff = diff / 2.0
                        midpoint = dhthigh + diff
                        if (gb.DIAG_LEVEL & 0x200):
                            gb.logging.info("RANGES do not overlap, with DHT reading lower")

        if (gb.DIAG_LEVEL & 0x200):
            gb.logging.info("Midpoint: %.1f" % (midpoint))
        return(midpoint)

    #----------------------------------
    # Send current day minimum and maximum temperature to monthly
    # averaging thread.  These are used to calculate running average
    # for minimum and maximum daily temperature
    #----------------------------------
    def get_current_day_min_max(self, month, month_str,
                                min_temp, max_temp, avg_q_out):

        gb.logging.info("Sending current day high/low to averaging thread")
        avg_msgType = avg.AVG_DAY_HIGH_LOW
        avgInfo = []
        avgInfo.append(avg_msgType)
        avgInfo.append(min_temp)
        avgInfo.append(max_temp)
        avgInfo.append(month)
        avgInfo.append(month_str)
        #if (gb.DIAG_LEVEL & 0x400):
        gb.logging.info("Sending %s(%d)" %
                     (avg.get_avg_msg_str(avg_msgType),avg_msgType))
        avg_q_out.put(avgInfo)

    #----------------------------------
    # Request last 1-day or last 30-day fields from currentreadings DB table
    #----------------------------------
    def init_last_ndays(self, db_q_out, ndays):
        db_msgType = db.DB_INIT_READINGS
        dbInfo = []
        dbInfo.append(db_msgType)
        dbInfo.append(ndays)
        if (gb.DIAG_LEVEL & 0x8):
            gb.logging.info("Sending %s(%d)" %
                     (db.get_db_msg_str(db_msgType),db_msgType))
        db_q_out.put(dbInfo)

    #----------------------------------
    # Request sunrise and sunset times
    #----------------------------------
    def request_sunrise_sunset(self, db_q_out, sun_date):
        db_msgType = db.DB_SUNTIMES
        dbInfo = []
        dbInfo.append(db_msgType)
        dbInfo.append(sun_date)
        if (gb.DIAG_LEVEL & 0x8):
            gb.logging.info("Sending %s(%d)" %
                     (db.get_db_msg_str(db_msgType),db_msgType))
        db_q_out.put(dbInfo)

    ############################################
    # ONLINE weather readings (McKelvey Park)
    ############################################
    #def get_remote_readings(self, db_q_out, avg_q_out):
    #    sp_weather = self.get_online_weather()
    #    sz = len(sp_weather)
    #    gb.logging.debug("list size: %d" % (sz))
    #    sp_humidity = 0
    #    sp_pressure = 0
    #    sp_temp = 0
    #    sp_wind = 0
    #    sp_deg = 0 # wind direction in degrees
    #    sp_gust = 0
    #    for ix in sp_weather:
    #        iy = ix.replace("{", "")
    #        iy = iy.replace("}", "")
    #        if "humidity" in iy:
    #            gb.logging.debug("%s" % (iy))
    #            sp_humidity = iy.split(' ')[2]
    #            #print(sp_humidity)
    #        elif "pressure" in iy:
    #            gb.logging.debug("%s" % (iy))
    #            sp_pressure = iy.split(' ')[2]
    #            #print(sp_pressure)
    #        elif "temp" in iy:
    #            if "min" not in iy and "max" not in iy:
    #                gb.logging.debug("%s" % (iy))
    #                sp_temp = iy.split(' ')[3]
    #                #print(sp_temp)
    #        elif "wind" in iy:
    #            sp_wind = iy.split(' ')[3]
    #            #print("Wind Speed: ", sp_wind)
    #        elif "deg" in iy:
    #            sp_deg = iy.split(' ')[2]
    #            #print("Wind Dir: ", sp_deg)
    #        elif "gust" in iy:
    #            sp_gust = iy.split(' ')[2]
    #            #print("Gusts: ", sp_gust)
    #        elif "dt" in iy:
    #            sp_date = iy.split(' ')[2]
    #            rmt_tm_str = gb.cvt_epoch_date_str_to_local_str(sp_date)
    #            #print("Date:", sp_date, ",", tm_str )
    #        elif "sunrise" in iy:
    #            sp_sr = iy.split(' ')[2]
    #            self.sunrise_str = gb.cvt_epoch_date_str_to_local_str(sp_sr)
    #            #print("Sunrise:", self.sunrise_str)
    #        elif "sunset" in iy:
    #            sp_ss = iy.split(' ')[2]
    #            self.sunset_str = gb.cvt_epoch_date_str_to_local_str(sp_ss)
    #            #print("Sunset: ", self.sunset_str)

    #    sp_temp_c = ((float(sp_temp) - 32.0) * 5.0) / 9.0
    #    if (gb.DIAG_LEVEL & 0x2):
    #        gb.logging.info("%s: McKelvey Park: %s mb, %s F, %s pct" %
    #                    (rmt_tm_str, sp_pressure, sp_temp, sp_humidity))

    #    db_msgType = db.DB_REMOTE_STATS
    #    dbInfo = []
    #    dbInfo.append(db_msgType)
    #    dbInfo.append(rmt_tm_str)
    #    dbInfo.append(sp_pressure)
    #    dbInfo.append(sp_temp)
    #    dbInfo.append(sp_temp_c)
    #    dbInfo.append(sp_humidity)
    #    dbInfo.append(sp_wind)
    #    dbInfo.append(sp_deg)
    #    dbInfo.append(sp_gust)
    #    dbInfo.append(self.sunrise_str)
    #    dbInfo.append(self.sunset_str)
    #    if (gb.DIAG_LEVEL & 0x8):
    #        gb.logging.info("Sending %s(%d)" %
    #                 (db.get_db_msg_str(db_msgType),db_msgType))
    #    db_q_out.put(dbInfo)

    #    monthly_msgType = avg.AVG_SUNTIMES
    #    monthlyInfo = []
    #    monthlyInfo.append(monthly_msgType)
    #    monthlyInfo.append(self.sunrise_str)
    #    monthlyInfo.append(self.sunset_str)
    #    if (gb.DIAG_LEVEL & 0x400):
    #        gb.logging.info("Sending %s(%d)" %
    #                 (avg.get_avg_msg_str(monthly_msgType),
    #                  monthly_msgType))
    #    avg_q_out.put(monthlyInfo)

    #    if (gb.DIAG_LEVEL & 0x1):
    #        gb.logging.info("Columbia: hPa/mb,inHg,mmHg,PSI; Sea Level: hPa/mb, difference from baseline; temp, humidity")

    #----------------------------------
    # Init min/max 1-day and 30-day readings in memory
    # with data collected from DB on startup
    #----------------------------------
    def init_min_max(self, weather_data):

        global MIN_DHT_F
        global MIN_DHT_TS
        global MIN_30_DHT_F
        global MIN_30_DHT_TS

        global MAX_DHT_F
        global MAX_DHT_TS
        global MAX_30_DHT_F
        global MAX_30_DHT_TS

        global MIN_HUMID
        global MIN_HUMID_TS
        global MIN_30_HUMID
        global MIN_30_HUMID_TS

        global MAX_HUMID
        global MAX_HUMID_TS
        global MAX_30_HUMID
        global MAX_30_HUMID_TS

        global MIN_MB
        global MIN_MB_TS
        global MIN_30_MB
        global MIN_30_MB_TS

        global MAX_MB
        global MAX_MB_TS
        global MAX_30_MB
        global MAX_30_MB_TS

        min_max_1day = weather_data[1]
        min_max_1day_ts = weather_data[2]
        min_max_30day = weather_data[3]
        min_max_30day_ts = weather_data[4]

        #print(min_max_1day)
        #print(min_max_1day_ts)
        #print(min_max_30day)
        #print(min_max_30day_ts)

        MIN_DHT_F = min_max_1day[0]
        MIN_DHT_TS = min_max_1day_ts[0]
        MIN_30_DHT_F = min_max_30day[0]
        MIN_30_DHT_TS = min_max_30day_ts[0]

        MAX_DHT_F = min_max_1day[1]
        MAX_DHT_TS = min_max_1day_ts[1]
        MAX_30_DHT_F = min_max_30day[1]
        MAX_30_DHT_TS = min_max_30day_ts[1]

        MIN_HUMID = min_max_1day[2]
        MIN_HUMID_TS = min_max_1day_ts[2]
        MIN_30_HUMID = min_max_30day[2]
        MIN_30_HUMID_TS = min_max_30day_ts[2]

        MAX_HUMID = min_max_1day[3]
        MAX_HUMID_TS = min_max_1day_ts[3]
        MAX_30_HUMID = min_max_30day[3]
        MAX_30_HUMID_TS = min_max_30day_ts[3]

        MIN_MB = min_max_1day[4]
        MIN_MB_TS = min_max_1day_ts[4]
        MIN_30_MB = min_max_30day[4]
        MIN_30_MB_TS = min_max_30day_ts[4]

        MAX_MB = min_max_1day[5]
        MAX_MB_TS = min_max_1day_ts[5]
        MAX_30_MB = min_max_30day[5]
        MAX_30_MB_TS = min_max_30day_ts[5]

    ########################################
    #
    # WeatherThread run function, including main loop
    # and received-message processing
    #
    ########################################
    def run(self):

        global DHT_SENSOR
        global BMP_SENSOR

        global HRS_24
        global DAYS_30

        global MAX_DHT_F
        global MAX_DHT_TS
        global MIN_DHT_F
        global MIN_DHT_TS

        global MAX_BMP_F
        global MAX_BMP_TS
        global MIN_BMP_F
        global MIN_BMP_TS

        global MIN_HUMID
        global MIN_HUMID_TS
        global MAX_HUMID
        global MAX_HUMID_TS

        global MIN_MB
        global MIN_MB_TS
        global MAX_MB
        global MAX_MB_TS

        global MAX_30_DHT_F
        global MAX_30_DHT_TS
        global MIN_30_DHT_F
        global MIN_30_DHT_TS

        global MAX_30_BMP_F
        global MAX_30_BMP_TS
        global MIN_30_BMP_F
        global MIN_30_BMP_TS

        global MAX_30_HUMID
        global MIN_30_HUMID
        global MAX_30_HUMID_TS
        global MIN_30_HUMID_TS

        global MAX_30_MB
        global MIN_30_MB
        global MAX_30_MB_TS
        global MIN_30_MB_TS

        MAX_TODAY_BMP_TS = gb.DFLT_TIME
        MAX_TODAY_BMP_F = gb.PRIOR_TEMP_DFLT

        MAX_TODAY_DHT_TS = gb.DFLT_TIME
        MAX_TODAY_DHT_F = gb.PRIOR_TEMP_DFLT

        MAX_TODAY_HUMID_TS = gb.DFLT_TIME
        MAX_TODAY_HUMID = wthr.MIN_DFLT_HUMID

        MAX_TODAY_MB_TS = gb.DFLT_TIME
        MAX_TODAY_MB = wthr.MAX_DFLT_MB

        MIN_TODAY_BMP_TS = gb.DFLT_TIME
        MIN_TODAY_BMP_F = MIN_DFLT_F

        MIN_TODAY_DHT_TS = gb.DFLT_TIME
        MIN_TODAY_DHT_F = MIN_DFLT_F

        MIN_TODAY_HUMID_TS = gb.DFLT_TIME
        MIN_TODAY_HUMID = wthr.MAX_DFLT_HUMID

        MIN_TODAY_MB_TS = gb.DFLT_TIME
        MIN_TODAY_MB = wthr.MIN_DFLT_MB

        wthr_q_in = self.args[0]
        db_q_out = self.args[1]
        avg_q_out = self.args[2]
        end_event = self.args[3]

        gb.logging.info("Running %s" % (self.name))
        gb.logging.debug(self.args)

        HRS_24 = 1.0 # 1 Days
        DAYS_30 = 30.0 # 1 Days

        dhtDevice = adafruit_dht.DHT22(DHT_PIN)

        # Using I2C bus for BMP280 (Barometric pressure sensor)
        
        gb.logging.info("Initializing BMP280")
        pressure_hPa = 0.00
        sea_level_hPa = 0.00
        bus = SMBus(1)
        sensor = BMP280(i2c_dev=bus)

        # Create the I2C interface.
        i2c = busio.I2C(SCL, SDA)

        gb.logging.info("BMP280 initialized")

        columbia_pressure = SEA_LEVEL_PRESSURE - self.get_hpa_from_feet(COLUMBIA_ALTITUDE_FT)

        local_iter = 0
        remote_iter = 0
        remote_iter_test = 0
        if (gb.DISABLE_RMT == True):
            # Disables calls to remote weather monitoring
            # -1 disables remote monitoring as remote_iter is either 0 or >0
            # Disable was originally used for testing purposes, so as not
            # to exceed allowable number of calls to weather service.
            # However, remote service frequently resulted in hangs so
            # remote use has been disabled and is no longer used
            remote_iter_test = -1
            gb.logging.info("REMOTE WEATHER CALLS DISABLED")
        else:
            gb.logging.info("REMOTE WEATHER calls ENABLED")

        min_dht = 0.0
        max_dht = 0.0
        min_30_dht = 0.0
        max_30_dht = 0.0

        min_bmp = 0.0
        max_bmp = 0.0
        min_30_bmp = 0.0
        max_30_bmp = 0.0

        min_humid = 0.0
        max_humid = 0.0
        min_30_humid = 0.0
        max_30_humid = 0.0

        min_mb = 0.0
        max_mb = 0.0
        min_30_mb = 0.0
        max_30_mb = 0.0

        bmp_min_ts_str = str(gb.DFLT_TIME)
        bmp_max_ts_str = str(gb.DFLT_TIME)
        dht_min_ts_str = str(gb.DFLT_TIME)
        dht_max_ts_str = str(gb.DFLT_TIME)
        humid_min_ts_str = str(gb.DFLT_TIME)
        humid_max_ts_str = str(gb.DFLT_TIME)
        mb_min_ts_str = str(gb.DFLT_TIME)
        mb_max_ts_str = str(gb.DFLT_TIME)

        bmp_min_30_ts_str = str(gb.DFLT_TIME)
        bmp_max_30_ts_str = str(gb.DFLT_TIME)
        dht_min_30_ts_str = str(gb.DFLT_TIME)
        dht_max_30_ts_str = str(gb.DFLT_TIME)
        humid_min_30_ts_str = str(gb.DFLT_TIME)
        humid_max_30_ts_str = str(gb.DFLT_TIME)
        mb_min_30_ts_str = str(gb.DFLT_TIME)
        mb_max_30_ts_str = str(gb.DFLT_TIME)

        current_day = 0

        init_in_progress = True

        DHT22_error = 0

        combo_temp = gb.PRIOR_TEMP_DFLT

        #--------------------------------
        # Initialize past-1 day/24 hour (currentreadings) and
        # last 30 days (readings30) database tables
        #--------------------------------
        self.init_last_ndays(db_q_out, 1)
        self.init_last_ndays(db_q_out, 30)
        gb.time.sleep(1)

        ########################################
        #
        #    WeatherThread init wait LOOP
        #
        ########################################

        while not end_event.isSet() and init_in_progress == True:
            while not wthr_q_in.empty():
                weather_data = wthr_q_in.get()
                weather_msgType = weather_data[0]

                if (weather_msgType == wthr.WTHR_INIT_COMPLETE):

                    # WTHR_INIT_COMPLETE indicates the readings30 and
                    # currentreadings tables have been updated.  Use
                    # this information to update in-memory current and
                    # 30-day global values

                    self.init_min_max(weather_data)
                    init_in_progress = False
                else:
                    gb.logging.error("Invalid message type: %d" %
                                     (weather_msgType))
                    gb.logging.error(weather_data)
            gb.time.sleep(2)

        gb.logging.info("1-Day & 30-Day readings initialized from DB")

        #----------------------------
        # Set simulate_yesterday to True to test day change
        #----------------------------
        simulate_yesterday = False

        # Initialize to now so cur_day is available within function
        # it will be reset to different day below
        cur_day = gb.datetime.now()

        ########################################
        #
        #    WeatherThread MAIN LOOP
        #
        ########################################
        while not end_event.isSet():

            ############################################
            # ONLINE weather readings (McKelvey Park)
            ############################################
            #if (remote_iter == remote_iter_test):
            #    self.get_remote_readings(db_q_out, avg_q_out)

            while not wthr_q_in.empty():
                weather_data = wthr_q_in.get()
                weather_msgType = weather_data[0]

                if (gb.DIAG_LEVEL & 0x3000):
                    gb.logging.info("Recvd: %s(%d)" %
                                    (wthr.get_weather_msg_str(weather_msgType),
                                     weather_msgType))

                if (weather_msgType == wthr.WTHR_INIT_CUR_MO):

                    # Restart running average monthly calculations for daytime
                    # and nighttime running averages when month changes

                    self.init_cur_mo_data(weather_data)

                elif (weather_msgType == wthr.WTHR_INIT_ALL_TIME):
                    self.init_all_time_data(weather_data)

                else:
                    gb.logging.error("Invalid weather message type: %d" %
                                     (weather_msgType))
                    gb.logging.error(weather_data)

            #---------------------------------------------
            # Set up current time (cur_time)... current_day is not updated
            # until after it is compared to day in cur_time
            #---------------------------------------------
            cur_time = gb.datetime.now()
            #gb.logging.info("current_day: %d" % (current_day))

            if (simulate_yesterday):

                gb.logging.info("current_day: %d" % (current_day))
                # Clause for testing day change
                cur_time = cur_time - gb.timedelta(days=1, hours=0)
                tm_str = gb.get_date_with_seconds(str(cur_time))

                # Set cur_day to year-month-day with time of 00:00:00
                cur_day = gb.datetime(cur_time.year, cur_time.month,
                                      cur_time.day - 1, 0, 0, 0)
                gb.logging.info("current_day: %d, cur_time.day: %d"
                                % (current_day, cur_time.day))
                print("cur_day: ", cur_day)
                simulate_yesterday = False

            else:
                tm_str = gb.get_date_with_seconds(str(cur_time))

                # Set cur_day to year-month-day with time of 00:00:00
                cur_day = gb.datetime(cur_time.year, cur_time.month,
                                      cur_time.day, 0, 0, 0)

            if (gb.DIAG_LEVEL & 0x1):
                gb.logging.info("current_day: %d, cur_time.day: %d, cur_day %s"
                                % (current_day, cur_time.day, str(cur_day)))

            if (cur_time.day != current_day):
                gb.logging.info("%s: DAY CHANGED from %d to %d" %
                                (tm_str, current_day, cur_time.day))
                gb.logging.info("current_day: %d, cur_time.day: %d" %
                                (current_day, cur_time.day))
                gb.logging.info("cur_day: %s" % (cur_day))

                #---------------------------------------------
                # On day change, get current days min/max readings
                # current_day should be 0 only on startup
                #---------------------------------------------
                if (current_day != 0):
                    # Get month string from cur_time
                    month_str = cur_time.strftime("%B")
                    gb.logging.info("current month: %s(%d)" %
                                    (month_str, cur_time.month))
                    self.get_current_day_min_max(cur_time.month, month_str,
                                                 MIN_DHT_F, MAX_DHT_F,
                                                 avg_q_out)
                current_day = cur_time.day
                gb.logging.info("current_day set to: %d" % (current_day))

                #---------------------------------------------
                # On day change, get current days sunrise and # sunset times
                #---------------------------------------------
                gb.logging.info("SUNRISE/SUNSET: tm_str: %s" % (tm_str))
                temp_date = gb.datetime.now()
                sun_date = temp_date.date()
                gb.logging.info("SUNRISE/SUNSET: sun_date: %s" % (sun_date))
                
                self.request_sunrise_sunset(db_q_out, sun_date)

            #---------------------------------------------
            # Get data from sensors every 20 seconds and store in
            # currentreadings table.  Store data in historical
            # readings table only once every 600 seconds (10 minutes)
            #---------------------------------------------
            if ((local_iter == 0) or ((local_iter % 2) == 0)):
                ############################################
                # BMP280 readings
                ############################################
                bmp_temp_c = sensor.get_temperature()
                bmp_temp_f = bmp_temp_c * 1.8 + 32
                pressure_hPa = sensor.get_pressure()
                altitude = sensor.get_altitude()

                #print("Raw: ", pressure_hPa)

                pressure_kPa = self.get_kPa(pressure_hPa)
                pressure_Pa = self.get_Pa(pressure_hPa)
                pressure_inHg = self.get_inHg(pressure_hPa)
                pressure_mmHg = self.get_mmHg(pressure_hPa)
                pressure_psi = self.get_psi(pressure_hPa)
                pressure_atm = self.get_atm(pressure_hPa, columbia_pressure)

                adjusted_sea_level = self.get_ft_adjusted_sea_level(
                                     pressure_hPa, COLUMBIA_ALTITUDE_FT)
                adjusted_sea_level_m = self.get_adjusted_sea_level(
                                     pressure_hPa, COLUMBIA_ALTITUDE_M)

                difference_from_sea_level = adjusted_sea_level - SEA_LEVEL_PRESSURE
                #altitude_difference = altitude - COLUMBIA_ALTITUDE_M
                altitude_difference = COLUMBIA_ALTITUDE_M - altitude
                altitude_difference_hPa = altitude_difference * hPa_per_meter
                columbia_dr_variance = pressure_hPa - columbia_pressure

                ############################################
                # DHT22 readings
                ############################################

                reading_count = 0

                # Readings can potentially fail, so retry
                # until a reading is obtained
                while(reading_count == 0):
                    try:
                        dht_temp_c = dhtDevice.temperature
                        humidity = dhtDevice.humidity

                        # Both temperature and humidity readdings obtained
                        dht_temp_f = dht_temp_c * (9.0/5.0) + 32.0
                        reading_count += 1
                        gb.logging.debug("%d: Temp: %.1f F / %.1f C, Humidity: %.1f" % (reading_count,dht_temp_f,dht_temp_c,humidity))
                    except RuntimeError as error:
                        # Errors happen fairly often, DHT's are hard to read,
                        # so just keep going
                        DHT22_error += 1
                        gb.logging.debug("%s: DHT WARNING with retry: %s" %
                                         (tm_str, error.args[0]))
                    gb.time.sleep(DHT_SLEEP_TIME)

                ############################################
                # "Combine" the DHT22 and BMP280 temperature readings
                ############################################
                combo_temp = self.get_combined_temperature(bmp_temp_f,
                                                           dht_temp_f)

            if ((local_iter == 0) or ((local_iter % 2) == 0)):
                #---------------------------------------------
                # Update readings table (history) in DB once every
                # 600 seconds:
                # - Sleep period is currently 10 seconds
                # - local_iter reset to 0 once 60 sleep periods occur
                #---------------------------------------------
                if (local_iter == 0):
                    tm_str = re.sub(' ', ",", tm_str)
                    if (gb.DIAG_LEVEL & 0x1):
                        gb.logging.info("%s; %.2f,%.3f,%.2f,%.2f; %.2f,%.2f; %.2f F,%.1f pct" % (tm_str,pressure_hPa,pressure_inHg,pressure_mmHg,pressure_psi,adjusted_sea_level,columbia_dr_variance,dht_temp_f,humidity))
                    db_msgType = db.DB_LOCAL_STATS
                    dbInfo = []
                    dbInfo.append(db_msgType)
                    dbInfo.append(tm_str)
                    dbInfo.append(pressure_hPa)
                    dbInfo.append(pressure_inHg)
                    dbInfo.append(pressure_mmHg)
                    dbInfo.append(pressure_psi)
                    dbInfo.append(adjusted_sea_level)
                    dbInfo.append(columbia_dr_variance)
                    dbInfo.append(bmp_temp_f)
                    dbInfo.append(bmp_temp_c)
                    dbInfo.append(dht_temp_f)
                    dbInfo.append(dht_temp_c)
                    dbInfo.append(humidity)
                    dbInfo.append(combo_temp)
                    if (gb.DIAG_LEVEL & 0x8):
                        gb.logging.info("Sending %s(%d)" %
                                 (db.get_db_msg_str(db_msgType),db_msgType))
                    db_q_out.put(dbInfo)

                    monthly_msgType = avg.AVG_TEMPF
                    monthlyInfo = []
                    monthlyInfo.append(monthly_msgType)
                    monthlyInfo.append(cur_time)
                    monthlyInfo.append(dht_temp_f)
                    if (gb.DIAG_LEVEL & 0x400):
                        gb.logging.info("Sending %s(%d)" %
                                 (avg.get_avg_msg_str(monthly_msgType),
                                  monthly_msgType))
                    avg_q_out.put(monthlyInfo)

                #---------------------------------------------
                # Update currentreadings in DB once every 20 seconds:
                # - Sleep period is currently 10 seconds
                # - mod 2 triggers update every other sleep period
                #---------------------------------------------
                if ((local_iter % 2) == 0):
                    #----------------
                    # Get Today max
                    #----------------
                    max_today_bmp = self.get_max_today_reading( 
                                               cur_time, bmp_temp_f,
                                               MAX_TODAY_BMP_TS,
                                               MAX_TODAY_BMP_F, cur_day)
                    if (max_today_bmp != gb.NO_CHANGE):
                        MAX_TODAY_BMP_F = max_today_bmp
                        MAX_TODAY_BMP_TS = MAX_DATE
                        bmp_max_today_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_TODAY_BMP_TS))
                        gb.logging.debug("MAX BMP: %s: %.1f F" %
                                    (bmp_max_today_ts_str, max_today_bmp))

                    max_today_dht = self.get_max_today_reading( 
                                               cur_time, dht_temp_f,
                                               MAX_TODAY_DHT_TS,
                                               MAX_TODAY_DHT_F, cur_day)
                    if (max_today_dht != gb.NO_CHANGE):
                        MAX_TODAY_DHT_F = max_today_dht
                        MAX_TODAY_DHT_TS = MAX_DATE
                        dht_max_today_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_TODAY_DHT_TS))

                        gb.logging.debug("MAX DHT: %s: %.1f F" %
                                    (dht_max_today_ts_str, max_today_dht))

                    max_today_humid = self.get_max_today_reading( 
                                               cur_time, humidity,
                                               MAX_TODAY_HUMID_TS,
                                               MAX_TODAY_HUMID, cur_day)
                    if (max_today_humid != gb.NO_CHANGE):
                        MAX_TODAY_HUMID = max_today_humid
                        MAX_TODAY_HUMID_TS = MAX_DATE
                        humid_max_today_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_TODAY_HUMID_TS))

                        gb.logging.debug("MAX Humidity Today: %s: %.1f pct" %
                                    (humid_max_today_ts_str, max_today_humid))

                    max_today_mb = self.get_max_today_reading(
                                               cur_time, pressure_hPa,
                                               MAX_TODAY_MB_TS,
                                               MAX_TODAY_MB, cur_day)
                    if (max_today_mb != gb.NO_CHANGE):
                        MAX_TODAY_MB = max_today_mb
                        MAX_TODAY_MB_TS = MAX_DATE
                        mb_max_today_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_TODAY_MB_TS))
                        gb.logging.debug("MAX MB Today: %s: %.1f mB" %
                                        (mb_max_today_ts_str, max_today_mb))

                    #----------------
                    # Get Today min
                    #----------------
                    min_today_bmp = self.get_min_today_reading(
                                               cur_time, bmp_temp_f,
                                               MIN_TODAY_BMP_TS,
                                               MIN_TODAY_BMP_F, cur_day)
                    if (min_today_bmp != gb.NO_CHANGE):
                        MIN_TODAY_BMP_F = min_today_bmp
                        MIN_TODAY_BMP_TS = MIN_DATE
                        bmp_min_today_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_TODAY_BMP_TS))
                        gb.logging.debug("MIN BMP: %s: %.1f F" %
                                    (bmp_min_today_ts_str, min_today_bmp))

                    min_today_dht = self.get_min_today_reading(
                                               cur_time, dht_temp_f,
                                               MIN_TODAY_DHT_TS,
                                               MIN_TODAY_DHT_F, cur_day)
                    if (min_today_dht != gb.NO_CHANGE):
                        MIN_TODAY_DHT_F = min_today_dht
                        MIN_TODAY_DHT_TS = MIN_DATE
                        dht_min_today_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_TODAY_DHT_TS))

                        gb.logging.debug("MIN DHT: %s: %.1f F" %
                                    (dht_min_today_ts_str, min_today_dht))

                    min_today_humid = self.get_min_today_reading(
                                               cur_time, humidity,
                                               MIN_TODAY_HUMID_TS,
                                               MIN_TODAY_HUMID, cur_day)
                    if (min_today_humid != gb.NO_CHANGE):
                        MIN_TODAY_HUMID = min_today_humid
                        MIN_TODAY_HUMID_TS = MIN_DATE
                        humid_min_today_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_TODAY_HUMID_TS))

                        gb.logging.debug("MIN Humidity Today: %s: %.1f pct" %
                                    (humid_min_today_ts_str, min_today_humid))

                    min_today_mb = self.get_min_today_reading(
                                               cur_time, pressure_hPa,
                                               MIN_TODAY_MB_TS,
                                               MIN_TODAY_MB, cur_day)
                    if (min_today_mb != gb.NO_CHANGE):
                        MIN_TODAY_MB = min_today_mb
                        MIN_TODAY_MB_TS = MIN_DATE
                        mb_min_today_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_TODAY_MB_TS))
                        gb.logging.debug("MIN MB Today: %s: %.1f mB" %
                                        (mb_min_today_ts_str, min_today_mb))

                    #----------------
                    # Get 24 HR max
                    #----------------
                    max_bmp = self.get_max_reading(cur_time, bmp_temp_f,
                                               MAX_BMP_TS, MAX_BMP_F, HRS_24)
                    if (max_bmp != gb.NO_CHANGE):
                        MAX_BMP_F = max_bmp
                        MAX_BMP_TS = MAX_DATE
                        bmp_max_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_BMP_TS))

                    max_dht = self.get_max_reading(cur_time, dht_temp_f,
                                               MAX_DHT_TS, MAX_DHT_F, HRS_24)
                    if (max_dht != gb.NO_CHANGE):
                        MAX_DHT_F = max_dht
                        MAX_DHT_TS = MAX_DATE
                        dht_max_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_DHT_TS))

                    gb.logging.debug("MAX_HUMID: %.1f" % (MAX_HUMID))
                    max_humid = self.get_max_reading(cur_time, humidity,
                                               MAX_HUMID_TS, MAX_HUMID, HRS_24)
                    if (max_humid != gb.NO_CHANGE):
                        gb.logging.debug("Updated max humidty 24hr: %.1f" %
                                                                 (max_humid))
                        MAX_HUMID = max_humid
                        MAX_HUMID_TS = MAX_DATE
                        humid_max_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_HUMID_TS))

                    max_mb = self.get_max_reading(cur_time, pressure_hPa,
                                               MAX_MB_TS, MAX_MB, HRS_24)
                    if (max_mb != gb.NO_CHANGE):
                        MAX_MB = max_mb
                        MAX_MB_TS = MAX_DATE
                        mb_max_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_MB_TS))

                    #----------------
                    # Get 24 HR min
                    #----------------
                    min_bmp = self.get_min_reading(cur_time, bmp_temp_f,
                                               MIN_BMP_TS, MIN_BMP_F, HRS_24)
                    if (min_bmp != gb.NO_CHANGE):
                        MIN_BMP_F = min_bmp
                        MIN_BMP_TS = MIN_DATE
                        bmp_min_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_BMP_TS))

                    min_dht = self.get_min_reading(cur_time, dht_temp_f,
                                               MIN_DHT_TS, MIN_DHT_F, HRS_24)
                    if (min_dht != gb.NO_CHANGE):
                        MIN_DHT_F = min_dht
                        MIN_DHT_TS = MIN_DATE
                        dht_min_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_DHT_TS))

                    gb.logging.debug("MIN_HUMID: %.1f" % (MIN_HUMID))
                    min_humid = self.get_min_reading(cur_time, humidity,
                                               MIN_HUMID_TS, MIN_HUMID, HRS_24)
                    if (min_humid != gb.NO_CHANGE):
                        gb.logging.debug("Updated min humidty 24hr: %.1f" %
                                                                (min_humid))
                        MIN_HUMID = min_humid
                        MIN_HUMID_TS = MIN_DATE
                        humid_min_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_HUMID_TS))

                    min_mb = self.get_min_reading(cur_time, pressure_hPa,
                                               MIN_MB_TS, MIN_MB, HRS_24)
                    if (min_mb != gb.NO_CHANGE):
                        MIN_MB = min_mb
                        MIN_MB_TS = MIN_DATE
                        mb_min_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_MB_TS))

                    #----------------
                    # Get 30 day max
                    #----------------
                    max_30_bmp = self.get_max_reading(cur_time, bmp_temp_f,
                                       MAX_30_BMP_TS, MAX_30_BMP_F, DAYS_30)
                    if (max_30_bmp != gb.NO_CHANGE):
                        MAX_30_BMP_F = max_30_bmp
                        MAX_30_BMP_TS = MAX_DATE
                        bmp_max_30_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_30_BMP_TS))

                    max_30_dht = self.get_max_reading(cur_time, dht_temp_f,
                                       MAX_30_DHT_TS, MAX_30_DHT_F, DAYS_30)
                    if (max_30_dht != gb.NO_CHANGE):
                        MAX_30_DHT_F = max_30_dht
                        MAX_30_DHT_TS = MAX_DATE
                        dht_max_30_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_30_DHT_TS))

                    gb.logging.debug("MAX_30_HUMID: %.1f" % (MAX_30_HUMID))
                    max_30_humid = self.get_max_reading(cur_time, humidity,
                                       MAX_30_HUMID_TS, MAX_30_HUMID, DAYS_30)
                    if (max_30_humid != gb.NO_CHANGE):
                        gb.logging.debug("Updated max humidty 30-days: %.1f" %
                                                                (max_30_humid))
                        MAX_30_HUMID = max_30_humid
                        MAX_30_HUMID_TS = MAX_DATE
                        humid_max_30_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_30_HUMID_TS))

                    max_30_mb = self.get_max_reading(cur_time, pressure_hPa,
                                           MAX_30_MB_TS, MAX_30_MB, DAYS_30)
                    if (max_30_mb != gb.NO_CHANGE):
                        MAX_30_MB = max_30_mb
                        MAX_30_MB_TS = MAX_DATE
                        mb_max_30_ts_str = gb.get_date_with_seconds(
                                                       str(MAX_30_MB_TS))

                    #----------------
                    # Get 30 day min
                    #----------------
                    min_30_bmp = self.get_min_reading(cur_time, bmp_temp_f,
                                       MIN_30_BMP_TS, MIN_30_BMP_F, DAYS_30)
                    if (min_30_bmp != gb.NO_CHANGE):
                        MIN_30_BMP_F = min_30_bmp
                        MIN_30_BMP_TS = MIN_DATE
                        bmp_min_30_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_30_BMP_TS))

                    min_30_dht = self.get_min_reading(cur_time, dht_temp_f,
                                       MIN_30_DHT_TS, MIN_30_DHT_F, DAYS_30)
                    if (min_30_dht != gb.NO_CHANGE):
                        MIN_30_DHT_F = min_30_dht
                        MIN_30_DHT_TS = MIN_DATE
                        dht_min_30_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_30_DHT_TS))

                    gb.logging.debug("MIN_30_HUMID: %.1f" % (MIN_30_HUMID))
                    min_30_humid = self.get_min_reading(cur_time, humidity,
                                       MIN_30_HUMID_TS, MIN_30_HUMID, DAYS_30)
                    if (min_30_humid != gb.NO_CHANGE):
                        gb.logging.debug("Updated min humidty 30-days: %.1f" %
                                                                (min_30_humid))
                        MIN_30_HUMID = min_30_humid
                        MIN_30_HUMID_TS = MIN_DATE
                        humid_min_30_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_30_HUMID_TS))

                    min_30_mb = self.get_min_reading(cur_time, pressure_hPa,
                                           MIN_30_MB_TS, MIN_30_MB, DAYS_30)
                    if (min_30_mb != gb.NO_CHANGE):
                        MIN_30_MB = min_30_mb
                        MIN_30_MB_TS = MIN_DATE
                        mb_min_30_ts_str = gb.get_date_with_seconds(
                                                       str(MIN_30_MB_TS))

                    gb.logging.debug("tm_str before: %s" % (tm_str))
                    tm_str = re.sub(' ', ",", tm_str)
                    gb.logging.debug("tm_str after: %s" % (tm_str))
                    bmp_min_ts_str = re.sub(' ', ",", bmp_min_ts_str)
                    bmp_max_ts_str = re.sub(' ', ",", bmp_max_ts_str)
                    dht_min_ts_str = re.sub(' ', ",", dht_min_ts_str)
                    dht_max_ts_str = re.sub(' ', ",", dht_max_ts_str)
                    humid_min_ts_str = re.sub(' ', ",", humid_min_ts_str)
                    humid_max_ts_str = re.sub(' ', ",", humid_max_ts_str)
                    mb_min_ts_str = re.sub(' ', ",", mb_min_ts_str)
                    mb_max_ts_str = re.sub(' ', ",", mb_max_ts_str)
                    bmp_min_30_ts_str = re.sub(' ', ",", bmp_min_30_ts_str)
                    bmp_max_30_ts_str = re.sub(' ', ",", bmp_max_30_ts_str)
                    dht_min_30_ts_str = re.sub(' ', ",", dht_min_30_ts_str)
                    dht_max_30_ts_str = re.sub(' ', ",", dht_max_30_ts_str)
                    humid_min_30_ts_str = re.sub(' ', ",", humid_min_30_ts_str)
                    humid_max_30_ts_str = re.sub(' ', ",", humid_max_30_ts_str)
                    mb_min_30_ts_str = re.sub(' ', ",", mb_min_30_ts_str)
                    mb_max_30_ts_str = re.sub(' ', ",", mb_max_30_ts_str)
                    bmp_min_today_ts_str = re.sub(' ', ",",
                                                  bmp_min_today_ts_str)
                    bmp_max_today_ts_str = re.sub(' ', ",",
                                                  bmp_max_today_ts_str)
                    dht_min_today_ts_str = re.sub(' ', ",",
                                                  dht_min_today_ts_str)
                    dht_max_today_ts_str = re.sub(' ', ",",
                                                  dht_max_today_ts_str)
                    humid_min_today_ts_str = re.sub(' ', ",",
                                                    humid_min_today_ts_str)
                    humid_max_today_ts_str = re.sub(' ', ",",
                                                    humid_max_today_ts_str)
                    mb_min_today_ts_str = re.sub(' ', ",",
                                                 mb_min_today_ts_str)
                    mb_max_today_ts_str = re.sub(' ', ",",
                                                 mb_max_today_ts_str)

                    if (gb.DIAG_LEVEL & 0x4):
                        gb.logging.info("BMP min: %.1f, TS: %s" %
                                                (min_bmp, bmp_min_ts_str))
                        gb.logging.info("BMP max: %.1f, TS: %s" %
                                                (max_bmp, bmp_max_ts_str))
                        gb.logging.info("DHT min: %.1f, TS: %s" %
                                                (min_dht, dht_min_ts_str))
                        gb.logging.info("DHT max: %.1f, TS: %s" %
                                                (max_dht, dht_max_ts_str))
                        gb.logging.info("HUMID min: %.1f, TS: %s" %
                                                (min_humid, humid_min_ts_str))
                        gb.logging.info("HUMID max: %.1f, TS: %s" %
                                                (max_humid, humid_max_ts_str))
                        gb.logging.info("MB min: %.1f, TS: %s" %
                                                (min_mb, mb_min_ts_str))
                        gb.logging.info("MB max: %.1f, TS: %s" %
                                                (max_mb, mb_max_ts_str))

                        gb.logging.info("BMP min 30: %.1f, TS: %s" %
                                                (min_30_bmp, bmp_min_30_ts_str))
                        gb.logging.info("BMP max 30: %.1f, TS: %s" %
                                                (max_30_bmp, bmp_max_ts_str))
                        gb.logging.info("DHT min 30: %.1f, TS: %s" %
                                                (min_30_dht, dht_min_30_ts_str))
                        gb.logging.info("DHT max 30: %.1f, TS: %s" %
                                                (max_30_dht, dht_max_30_ts_str))
                        gb.logging.info("HUMID min 30: %.1f, TS: %s" %
                                            (min_30_humid, humid_min_30_ts_str))
                        gb.logging.info("HUMID max 30: %.1f, TS: %s" %
                                            (max_30_humid, humid_max_30_ts_str))
                        gb.logging.info("MB min 30: %.1f, TS: %s" %
                                            (min_30_mb, humid_min_30_ts_str))
                        gb.logging.info("MB max 30: %.1f, TS: %s" %
                                            (max_30_mb, humid_max_30_ts_str))

                    db_msgType = db.DB_CUR_STATS
                    dbInfo = []
                    dbInfo.append(db_msgType)            #00
                    dbInfo.append(tm_str)
                    dbInfo.append(pressure_hPa)
                    dbInfo.append(min_mb)
                    dbInfo.append(mb_min_ts_str)
                    dbInfo.append(max_mb)
                    dbInfo.append(mb_max_ts_str)
                    dbInfo.append(bmp_temp_f)
                    dbInfo.append(min_bmp)
                    dbInfo.append(bmp_min_ts_str)
                    dbInfo.append(max_bmp)               #10
                    dbInfo.append(bmp_max_ts_str)
                    dbInfo.append(dht_temp_f)
                    dbInfo.append(min_dht)
                    dbInfo.append(dht_min_ts_str)
                    dbInfo.append(max_dht)
                    dbInfo.append(dht_max_ts_str)
                    dbInfo.append(humidity)
                    dbInfo.append(min_humid)
                    dbInfo.append(humid_min_ts_str)
                    dbInfo.append(max_humid)             #20
                    dbInfo.append(humid_max_ts_str)
                    dbInfo.append(min_30_bmp)
                    dbInfo.append(bmp_min_30_ts_str)
                    dbInfo.append(max_30_bmp)
                    dbInfo.append(bmp_max_30_ts_str)
                    dbInfo.append(min_30_dht)
                    dbInfo.append(dht_min_30_ts_str)
                    dbInfo.append(max_30_dht)
                    dbInfo.append(dht_max_30_ts_str)
                    dbInfo.append(min_30_humid)          #30
                    dbInfo.append(humid_min_30_ts_str)
                    dbInfo.append(max_30_humid)
                    dbInfo.append(humid_max_30_ts_str)
                    dbInfo.append(min_30_mb)
                    dbInfo.append(mb_min_30_ts_str)
                    dbInfo.append(max_30_mb)
                    dbInfo.append(mb_max_30_ts_str)
                    dbInfo.append(min_today_bmp)
                    dbInfo.append(bmp_min_today_ts_str)
                    dbInfo.append(max_today_bmp)         #40
                    dbInfo.append(bmp_max_today_ts_str)
                    dbInfo.append(min_today_dht)
                    dbInfo.append(dht_min_today_ts_str)
                    dbInfo.append(max_today_dht)
                    dbInfo.append(dht_max_today_ts_str)
                    dbInfo.append(min_today_humid)
                    dbInfo.append(humid_min_today_ts_str)
                    dbInfo.append(max_today_humid)
                    dbInfo.append(humid_max_today_ts_str)
                    dbInfo.append(min_today_mb)          #50
                    dbInfo.append(mb_min_today_ts_str)
                    dbInfo.append(max_today_mb)
                    dbInfo.append(mb_max_today_ts_str)   #53

                    if (gb.DIAG_LEVEL & 0x8):
                        gb.logging.info("Sending %s(%d)" %
                                 (db.get_db_msg_str(db_msgType),db_msgType))
                    db_q_out.put(dbInfo)

                    self.get_cur_month_min_max(cur_time, dht_temp_f,
                                               pressure_hPa, humidity,
                                               db_q_out, end_event)
                    self.get_all_time_month_min_max(cur_time, dht_temp_f,
                                                    pressure_hPa, humidity,
                                                    db_q_out, end_event)

            remote_iter += 1
            if (remote_iter >= REMOTE_FREQUENCY):
                remote_iter = 0

            local_iter += 1
            if (local_iter >= LOCAL_FREQUENCY):
                local_iter = 0

            gb.logging.debug("%s: local_iter: %d, remote_iter: %d" %
                                      (tm_str, local_iter, remote_iter))

            gb.time.sleep(END_EVENT_CHECK)

        gb.logging.info("Exiting %s" % (self.name))
        return
