import gb
import db
import fan
import wthr

# mariadb imports
import mysql.connector as mariadb
import sys
from decimal import Decimal

DB_SLEEP = 5
DB_SUCCESS = 1

PRIOR_CPU_TEMPC = gb.PRIOR_TEMP_DFLT

MIN_MAX_FIELDS = [ "tempdhtf", "tempdhtf", "humidity", "humidity", "baromB", "baromB" ]
MIN_MAX_TS_FIELDS = [ "mindhtts", "maxdhtts", "minhumts", "maxhumts", "lowmbts", "highmbts" ]
MIN_MAX_UPDATE_FIELDS = [ "mindhtf", "maxdhtf", "minhumidity", "maxhumidity", "lowmbts", "highmbts" ]

temp_min_update_sql = """UPDATE currentreadings SET mindhtf=%s, mindhtts=%s WHERE id=1"""
temp_max_update_sql = """UPDATE currentreadings SET maxdhtf=%s, maxdhtts=%s WHERE id=1"""
humid_min_update_sql = """UPDATE currentreadings SET minhumidity=%s, minhumts=%s WHERE id=1"""
humid_max_update_sql = """UPDATE currentreadings SET maxhumidity=%s, maxhumts=%s WHERE id=1"""
baro_min_update_sql = """UPDATE currentreadings SET lowmB=%s, lowmbts=%s WHERE id=1"""
baro_max_update_sql = """UPDATE currentreadings SET highmB=%s, highmbts=%s WHERE id=1"""

temp_min_30_update_sql = """UPDATE readings30 SET min30dhtf=%s, min30dhtts=%s WHERE id=1"""
temp_max_30_update_sql = """UPDATE readings30 SET max30dhtf=%s, max30dhtts=%s WHERE id=1"""
humid_min_30_update_sql = """UPDATE readings30 SET min30humidity=%s, min30humts=%s WHERE id=1"""
humid_max_30_update_sql = """UPDATE readings30 SET max30humidity=%s, max30humts=%s WHERE id=1"""
baro_min_30_update_sql = """UPDATE readings30 SET low30mB=%s, low30mbts=%s WHERE id=1"""
baro_max_30_update_sql = """UPDATE readings30 SET high30mB=%s, high30mbts=%s WHERE id=1"""

SQL_UPDT_1DAY = []
SQL_UPDT_30DAY = []

MAX_1DAY     = [0.0] * 6
MAX_1DAY_TS  = [gb.DFLT_TIME] * 6
MAX_30DAY    = [0.0] * 6
MAX_30DAY_TS = [gb.DFLT_TIME] * 6

DAYS1 = 1
DAYS30 = 30

#######################################################################
#
# Database Thread (74hc165 shift register thread)
#
#######################################################################
class DatabaseThread(gb.threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        gb.threading.Thread.__init__(self, group=group, target=target, name=name)
        gb.logging.info("Setting up Database thread")
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.db_conn = 0
        self.db_cursor = 0

    #-----------------------------
    # Update LOCALLY collected data
    #-----------------------------
    def db_update_local(self, db_data):
        gb.logging.debug("db_update_local")
        #print(db_data)
        tm_str = db_data[1]
        pressure_hPa = db_data[2]
        pressure_inHg = db_data[3]
        pressure_mmHg = db_data[4]                # not stored in DB
        pressure_psi = db_data[5]                 # not stored in DB
        pressure_adjusted_sea_level = db_data[6]  # not stored in DB
        columbia_dr_variance = db_data[7]
        bmp_temp_f = db_data[8]
        bmp_temp_c = db_data[9]
        dht_temp_f = db_data[10]
        dht_temp_c = db_data[11]
        dht_humidity = db_data[12]
        temp_combined = db_data[13]

        db_id = 10
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("REPLACE INTO readings; db_id: %d" % (db_id))

#            add_local_weather = ("REPLACE INTO readings "
#                                 "(recordType, tmstamp, baromB, baroinHg, "
#                                 "barommHg, baroPsi, baroVariance, "
#                                 "baroSeaLvl, tempbmpf, tempbmpc, humidity, "
#                                 "tempdhtc, tempdhtf) "
#                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
#            data_add = ('LOCAL', tm_str, pressure_hPa, pressure_inHg, pressure_mmHg, pressure_psi, columbia_dr_variance, pressure_adjusted_sea_level, bmp_temp_f, bmp_temp_c, dht_humidity, dht_temp_c, dht_temp_f)

            add_local_weather = ("REPLACE INTO readings "
                                 "(recordType, tmstamp, baromB, baroinHg, "
                                 "baroVariance, tempbmpf, tempbmpc, humidity, "
                                 "tempdhtc, tempdhtf, tempCombined) "
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_add = ('LOCAL', tm_str, pressure_hPa, pressure_inHg, columbia_dr_variance, bmp_temp_f, bmp_temp_c, dht_humidity, dht_temp_c, dht_temp_f, temp_combined)

            self.db_cursor.execute(add_local_weather, data_add)
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x20):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #-----------------------------
    # Update web-based, RREMOTELY collected data
    # (GPS coordinates indicate McKelvey Park area)
    #-----------------------------
    def db_update_remote(self, db_data):
        gb.logging.debug("db_update_remote")
        #print(db_data)
        tm_str = db_data[1]
        pressure_hPa = db_data[2]
        temp_f = db_data[3]
        temp_c = db_data[4]
        humidity = db_data[5]
        wind_speed = db_data[6]
        wind_degrees = db_data[7]
        wind_gusts = db_data[8]
        sunrise = db_data[9]
        sunset = db_data[10]

        db_id = 11
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("REPLACE INTO readings; db_id: %d" % (db_id))

            add_remote_weather = ("REPLACE INTO readings "
                                 "(recordType, tmstamp, baromB, "
                                 "tempdhtf, tempdhtc, humidity) "
                                 "VALUES (%s, %s, %s, %s, %s, %s)")
            data_add = ('REMOTE', tm_str, pressure_hPa, temp_f, temp_c, humidity)

            self.db_cursor.execute(add_remote_weather, data_add)
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x20):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 61
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("REPLACE INTO readings; db_id: %d" % (db_id))

            add_remote_weather = ("REPLACE INTO rmt_readings "
                                 "(tmstamp, baromBrmt, "
                                 "temprmtf, humidityrmt, windspeed, winddir, "
                                 "windgust, sunrise, sunset) "
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_add = (tm_str, pressure_hPa, temp_f, humidity, wind_speed, wind_degrees, wind_gusts, sunrise, sunset)

            self.db_cursor.execute(add_remote_weather, data_add)
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x20):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #############################################################
    # Update CPU fan state in database
    # DB_CPU_FAN
    #############################################################
    def db_update_cpu_fan(self, db_data):

        global DB_UPDATE
        global DB_COMMIT

        tm_str = db_data[1]
        fan_st = db_data[2]

        pri_key = 1

        db_id = 20
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("UPDATE cpulatest; db_id: %d" % (db_id))
            
            self.db_cursor.execute("""UPDATE cpulatest SET fanchangetm=%s, cpufanstate=%s WHERE id=%s""", (tm_str, fan_st, pri_key))
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 21
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("REPLACE INTO historycputemp; db_id: %d" %
                               (db_id))

            add_history = ("REPLACE INTO historycputemp "
                                  "(tmstamp, cpufanstate) "
                                  "VALUES (%s, %s)")
            data_add = (tm_str, fan_st)

            self.db_cursor.execute(add_history, data_add)
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR 2049: %s" % (e))

    #############################################################
    # Update CPU temperature in database
    # DB_CPU_TEMPERATURE
    #############################################################
    def db_update_cpu_temperature(self, db_data):

        global DB_UPDATE
        global DB_COMMIT
        global PRIOR_CPU_TEMPC

        tm_str = db_data[1]
        cpu_c = db_data[2]
        fan_st = db_data[3]

        pri_key = 1

        db_id = 22
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("UPDATE cpulatest; db_id: %d" % (db_id))

            self.db_cursor.execute("""UPDATE cpulatest SET cputempc=%s WHERE id=%s""", (cpu_c, pri_key))
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        # Update temperature history only if change greater
        # FAN_TEMPERATURE_DIFF_DB
        if (abs(PRIOR_CPU_TEMPC - cpu_c) >= fan.FAN_TEMPERATURE_DIFF_DB or
            (PRIOR_CPU_TEMPC == gb.PRIOR_TEMP_DFLT)):
            PRIOR_CPU_TEMPC = cpu_c

            db_id = 23
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("REPLACE INTO historycputemp; db_id: %d" %
                               (db_id))

                add_history = ("REPLACE INTO historycputemp "
                               "(tmstamp, cputempc, cpufanstate) "
                               "VALUES (%s, %s, %s)")
                data_add = (tm_str, cpu_c, fan_st)

                self.db_cursor.execute(add_history, data_add)
                self.db_cursor.execute("COMMIT")

            #    if (gb.DIAG_LEVEL & 0x4):
            #        DB_UPDATE = DB_UPDATE + 1
            #        DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    def db_current_stats(self, db_data):
        global DB_UPDATE
        global DB_COMMIT

        if (gb.DIAG_LEVEL & 0x10):
            gb.logging.info("db_current_stats")

        tm_str            = db_data[1]
        pressure_hPa      = db_data[2]
        mb_min            = db_data[3]
        mb_min_ts_str     = db_data[4]
        mb_max            = db_data[5]
        mb_max_ts_str     = db_data[6]
        bmp_temp_f        = db_data[7]
        bmp_min           = db_data[8]
        bmp_min_ts_str    = db_data[9]
        bmp_max           = db_data[10]
        bmp_max_ts_str    = db_data[11]
        dht_temp_f        = db_data[12]
        dht_min           = db_data[13]
        dht_min_ts_str    = db_data[14]
        dht_max           = db_data[15]
        dht_max_ts_str    = db_data[16]
        humidity          = db_data[17]
        humid_min         = db_data[18]
        humid_min_ts_str  = db_data[19]
        humid_max         = db_data[20]
        humid_max_ts_str  = db_data[21]
        bmp_30_min        = db_data[22]
        bmp_30_min_ts_str = db_data[23]
        bmp_30_max        = db_data[24]
        bmp_30_max_ts_str = db_data[25]
        dht_30_min        = db_data[26]
        dht_30_min_ts_str = db_data[27]
        dht_30_max        = db_data[28]
        dht_30_max_ts_str = db_data[29]
        humid_30_min      = db_data[30]
        humid_30_min_ts_str = db_data[31]
        humid_30_max      = db_data[32]
        humid_30_max_ts_str = db_data[33]
        mb_30_min         = db_data[34]
        mb_30_min_ts_str  = db_data[35]
        mb_30_max         = db_data[36]
        mb_30_max_ts_str  = db_data[37]
        bmp_Today_min        = db_data[38]
        bmp_Today_min_ts_str = db_data[39]
        bmp_Today_max        = db_data[40]
        bmp_Today_max_ts_str = db_data[41]
        dht_Today_min        = db_data[42]
        dht_Today_min_ts_str = db_data[43]
        dht_Today_max        = db_data[44]
        dht_Today_max_ts_str = db_data[45]
        humid_Today_min      = db_data[46]
        humid_Today_min_ts_str = db_data[47]
        humid_Today_max      = db_data[48]
        humid_Today_max_ts_str = db_data[49]
        mb_Today_min         = db_data[50]
        mb_Today_min_ts_str  = db_data[51]
        mb_Today_max         = db_data[52]
        mb_Today_max_ts_str  = db_data[53]

        pri_key = 1

        if (bmp_min != gb.NO_CHANGE):
            db_id = 31
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET minbmpf=%s, minbmpts=%s WHERE id=%s""", (bmp_min, bmp_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (bmp_max != gb.NO_CHANGE):
            db_id = 32
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET maxbmpf=%s, maxbmpts=%s WHERE id=%s""", (bmp_max, bmp_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_min != gb.NO_CHANGE):
            db_id = 33
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET mindhtf=%s, mindhtts=%s WHERE id=%s""", (dht_min, dht_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_max != gb.NO_CHANGE):
            db_id = 34
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET maxdhtf=%s, maxdhtts=%s WHERE id=%s""", (dht_max, dht_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_min != gb.NO_CHANGE):
            db_id = 35
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET minhumidity=%s, minhumts=%s WHERE id=%s""", (humid_min, humid_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_max != gb.NO_CHANGE):
            db_id = 36
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET maxhumidity=%s, maxhumts=%s WHERE id=%s""", (humid_max, humid_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_min != gb.NO_CHANGE):
            db_id = 37
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET lowmB=%s, lowmbts=%s WHERE id=%s""", (mb_min, mb_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_max != gb.NO_CHANGE):
            db_id = 38
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE currentreadings SET highmB=%s, highmbts=%s WHERE id=%s""", (mb_max, mb_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 30
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("UPDATE currentreadings; db_id: %d" % (db_id))
            
            self.db_cursor.execute("""UPDATE currentreadings SET tmstamp=%s, tempbmpf=%s, tempdhtf=%s, humidity=%s, baromB=%s WHERE id=%s""", (tm_str, bmp_temp_f, dht_temp_f, humidity, pressure_hPa, pri_key))
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))


        #-------------------
        # 30-day min/max
        #-------------------
        if (bmp_30_min != gb.NO_CHANGE):
            db_id = 39
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET min30bmpf=%s, min30bmpts=%s WHERE id=%s""", (bmp_30_min, bmp_30_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (bmp_30_max != gb.NO_CHANGE):
            db_id = 40
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET max30bmpf=%s, max30bmpts=%s WHERE id=%s""", (bmp_30_max, bmp_30_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_30_min != gb.NO_CHANGE):
            db_id = 41
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET min30dhtf=%s, min30dhtts=%s WHERE id=%s""", (dht_30_min, dht_30_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_30_max != gb.NO_CHANGE):
            db_id = 42
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET max30dhtf=%s, max30dhtts=%s WHERE id=%s""", (dht_30_max, dht_30_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_30_min != gb.NO_CHANGE):
            db_id = 43
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET min30humidity=%s, min30humts=%s WHERE id=%s""", (humid_30_min, humid_30_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_30_max != gb.NO_CHANGE):
            db_id = 44
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET max30humidity=%s, max30humts=%s WHERE id=%s""", (humid_30_max, humid_30_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_30_min != gb.NO_CHANGE):
            db_id = 45
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET low30mB=%s, low30mbts=%s WHERE id=%s""", (mb_30_min, mb_30_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_30_max != gb.NO_CHANGE):
            db_id = 46
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readings30; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readings30 SET high30mB=%s, high30mbts=%s WHERE id=%s""", (mb_30_max, mb_30_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        #-------------------
        # Today min/max
        #-------------------
        if (bmp_Today_min != gb.NO_CHANGE):
            db_id = 57
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET minTodaybmpf=%s, minTodaybmpts=%s WHERE id=%s""", (bmp_Today_min, bmp_Today_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (bmp_Today_max != gb.NO_CHANGE):
            db_id = 58
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET maxTodaybmpf=%s, maxTodaybmpts=%s WHERE id=%s""", (bmp_Today_max, bmp_Today_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_Today_min != gb.NO_CHANGE):
            db_id = 59
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET minTodaydhtf=%s, minTodaydhtts=%s WHERE id=%s""", (dht_Today_min, dht_Today_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (dht_Today_max != gb.NO_CHANGE):
            db_id = 60
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET maxTodaydhtf=%s, maxTodaydhtts=%s WHERE id=%s""", (dht_Today_max, dht_Today_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_Today_min != gb.NO_CHANGE):
            db_id = 62
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET minTodayhumidity=%s, minTodayhumts=%s WHERE id=%s""", (humid_Today_min, humid_Today_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (humid_Today_max != gb.NO_CHANGE):
            db_id = 63
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET maxTodayhumidity=%s, maxTodayhumts=%s WHERE id=%s""", (humid_Today_max, humid_Today_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_Today_min != gb.NO_CHANGE):
            db_id = 64
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET lowTodaymB=%s, lowTodaymbts=%s WHERE id=%s""", (mb_Today_min, mb_Today_min_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        if (mb_Today_max != gb.NO_CHANGE):
            db_id = 65
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE readingsToday; db_id: %d" % (db_id))
                self.db_cursor.execute("""UPDATE readingsToday SET highTodaymB=%s, highTodaymbts=%s WHERE id=%s""", (mb_Today_max, mb_Today_max_ts_str, pri_key))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #-----------------------------------------
    #
    #-----------------------------------------
    def get_date_from_reading(self, field, reading, ndays):

        global DB_SUCCESS
        global DB_SELECT


        db_id = 49
        # Configure error response
        resp = []
        resp.append(-db_id)
        resp.append(gb.DFLT_TIME)

        sql_var = "SELECT tmstamp FROM readings WHERE {0}={1} and tmstamp >= NOW() - INTERVAL {2} DAY ORDER by tmstamp DESC".format(field, reading, ndays)
        gb.logging.debug("date_from_reading SQL: %s" % (sql_var))

        try:
            select = self.db_cursor.execute(sql_var)
            select_data = self.db_cursor.fetchall()
            records = self.db_cursor.rowcount

            if (records != 0):
                for row in range(records):
                    if (row == 0):
                        ts_date = select_data[row]

                if (gb.DIAG_LEVEL & 0x40):
                    gb.logging.debug("Timestamp 1: %s" % (ts_date[0]))
                resp[0] = DB_SUCCESS
                resp[1] = ts_date

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        return(resp)

    #-----------------------------------------
    #
    #-----------------------------------------
    def db_init_currentreadings(self, db_data):

        global DB_SUCCESS
        global DB_SELECT
        global SQL_UPDT_1DAY
        global MAX_1DAY
        global MAX_1DAY_TS
        global self_db_conn

        db_error = DB_SUCCESS;

        db_id = 47
        err_resp = -db_id

        sql_select = 'SELECT min(tempdhtf), max(tempdhtf), min(humidity), max(humidity), min(baromB), max(baromB) from readings where recordType="LOCAL" and tmstamp >= NOW() - INTERVAL 1 DAY'
        try:
            select = self.db_cursor.execute(sql_select)
            select_data = self.db_cursor.fetchone()
            
            if (gb.DIAG_LEVEL & 0x40):
                print("\nSELECT resp: ", select_data, "\n")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_SELECT = DB_SELECT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))
            db_error = err_resp

        if (db_error == DB_SUCCESS):
            elements_in_select_data = len(select_data)
            if (gb.DIAG_LEVEL & 0x40):
                gb.logging.info("elements_in_select_data: %d" %
                                (elements_in_select_data))
            for fld in range(elements_in_select_data):

                ts_resp = self.get_date_from_reading(MIN_MAX_FIELDS[fld],
                                                     select_data[fld],
                                                     DAYS1)
                if (ts_resp[0] != DB_SUCCESS):
                    gb.logging.error("ERROR %d: unable to find timestamp: %d" %
                                 (db_id, ts_resp[0]))
                else:
                    timestamp = ts_resp[1]

                    db_id = 67
                    try:
                        input_data = (select_data[fld], timestamp[0])
                        self.db_cursor.execute(SQL_UPDT_1DAY[fld], input_data)
                        self_db_conn.commit()

                        if (gb.DIAG_LEVEL & 0x80):
                            print("CURR SQL[", fld, "]: ", SQL_UPDT_1DAY[fld])
                        if (gb.DIAG_LEVEL & 0x40):
                            gb.logging.info("Updated 1 day %s: %s %.1f" %
                                            (MIN_MAX_FIELDS[fld], timestamp[0],
                                             select_data[fld]))
                        MAX_1DAY[fld] = float(select_data[fld])
                        MAX_1DAY_TS[fld] = timestamp[0]

                    except mariadb.Error as e:
                        gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

            if (gb.DIAG_LEVEL & 0x40):
                print(MAX_1DAY)
                print(MAX_1DAY_TS)

    #-----------------------------------------
    #
    #-----------------------------------------
    def db_init_readings30(self, db_data):

        global DB_SUCCESS
        global DB_SELECT
        global SQL_UPDT_30DAY
        global MAX_30DAY
        global MAX_30DAY_TS
        global self_db_conn

        db_error = DB_SUCCESS;

        db_id = 48
        err_resp = -db_id
        try:
            select = self.db_cursor.execute('SELECT min(tempdhtf), max(tempdhtf), min(humidity), max(humidity), min(baromB), max(baromB) from readings where recordType="LOCAL" and tmstamp >= NOW() - INTERVAL 30 DAY')
            select_data = self.db_cursor.fetchone()

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_SELECT = DB_SELECT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))
            db_error = err_resp

        if (db_error == DB_SUCCESS):
            elements_in_select_data = len(select_data)
            if (gb.DIAG_LEVEL & 0x40):
                gb.logging.info("elements_in_select_data: %d" %
                                (elements_in_select_data))
            for fld in range(elements_in_select_data):
                ts_resp = self.get_date_from_reading(MIN_MAX_FIELDS[fld],
                                                     select_data[fld],
                                                     DAYS30)
                if (ts_resp[0] != DB_SUCCESS):
                    gb.logging.error("ERROR %d: unable to find timestamp: %d" %
                                 (db_id, ts_resp[0]))
                else:
                    timestamp = ts_resp[1]

                    db_id = 68
                    try:
                        input_data = (select_data[fld], timestamp[0])
                        self.db_cursor.execute(SQL_UPDT_30DAY[fld], input_data)
                        self_db_conn.commit()

                        if (gb.DIAG_LEVEL & 0x80):
                            print("CURR SQL[", fld, "]: ", SQL_UPDT_30DAY[fld])
                        if (gb.DIAG_LEVEL & 0x40):
                            gb.logging.info("Updated 30 day %s: %s %.1f" %
                                            (MIN_MAX_FIELDS[fld], timestamp[0],
                                             select_data[fld]))
                        MAX_30DAY[fld] = float(select_data[fld])
                        MAX_30DAY_TS[fld] = timestamp[0]

                    except mariadb.Error as e:
                        gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

            if (gb.DIAG_LEVEL & 0x40):
                print(MAX_30DAY)
                print(MAX_30DAY_TS)

    #############################################################
    # Database run function
    #############################################################
    def run(self):

        global self_db_conn
        global SQL_UPDT_1DAY
        global SQL_UPDT_30DAY
        global MAX_1DAY
        global MAX_1DAY_TS
        global MAX_30DAY
        global MAX_30DAY_TS

        db_q_in = self.args[0]
        cpufan_q_out = self.args[1]
        wthr_q_out = self.args[2]
        end_event = self.args[3]

        gb.logging.info("Running %s" % (self.name))
        gb.logging.debug(self.args)

        init_1 = False
        init_30 = False
        not_sent = True

        try:
            self_db_conn = mariadb.connect(
                user="pi",
                password="tashA2020!",
                host="localhost",
                port=3306,
                database="weather"
            )

            self_db_conn.autocommit = False
            gb.logging.info("Database autocommit DISABLED, DIAG_LEVEL 0x%x" %
                         (gb.DIAG_LEVEL))

            self.db_cursor = self_db_conn.cursor()

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR 0: %s" % (e))
            sys.exit(1)

        SQL_UPDT_1DAY.append(temp_min_update_sql)
        SQL_UPDT_1DAY.append(temp_max_update_sql)
        SQL_UPDT_1DAY.append(humid_min_update_sql)
        SQL_UPDT_1DAY.append(humid_max_update_sql)
        SQL_UPDT_1DAY.append(baro_min_update_sql)
        SQL_UPDT_1DAY.append(baro_max_update_sql)

        SQL_UPDT_30DAY.append(temp_min_30_update_sql)
        SQL_UPDT_30DAY.append(temp_max_30_update_sql)
        SQL_UPDT_30DAY.append(humid_min_30_update_sql)
        SQL_UPDT_30DAY.append(humid_max_30_update_sql)
        SQL_UPDT_30DAY.append(baro_min_30_update_sql)
        SQL_UPDT_30DAY.append(baro_max_30_update_sql)

        ###################################################
        # DB main loop: Part 1: INCOMING MESSAGES
        ###################################################
        while not end_event.isSet():

            #gb.logging.info("DB Part 1: Check for incoming DB msgs")

            while not db_q_in.empty():
                db_data = db_q_in.get()
                db_msgType = db_data[0]
                gb.logging.debug(db_data)

                if (db_msgType == db.DB_CUR_STATS):
                    self.db_current_stats(db_data)

                elif (db_msgType == db.DB_LOCAL_STATS):
                    self.db_update_local(db_data)

                elif (db_msgType == db.DB_REMOTE_STATS):
                    self.db_update_remote(db_data)

                elif (db_msgType == db.DB_CPU_TEMPERATURE):
                    self.db_update_cpu_temperature(db_data)

                elif (db_msgType == db.DB_CPU_FAN):
                    self.db_update_cpu_fan(db_data)

                elif (db_msgType == db.DB_INIT_READINGS):
                    if (db_data[1] == 1):
                        self.db_init_currentreadings(db_data)
                        init_1 = True
                    else:
                        self.db_init_readings30(db_data)
                        init_30 = True

                else:
                    gb.logging.error("Invalid DB message type: %d" %
                                                          (db_msgType))
                    gb.logging.error(db_data)

            if (not_sent and init_1 and init_30): 
                # Let weather.py know initiailization is complete
                wthrInfo = []
                wthr_msgType = wthr.WTHR_INIT_COMPLETE
                wthrInfo.append(wthr_msgType)
                wthrInfo.append(MAX_1DAY)
                wthrInfo.append(MAX_1DAY_TS)
                wthrInfo.append(MAX_30DAY)
                wthrInfo.append(MAX_30DAY_TS)

                if (gb.DIAG_LEVEL & 0x8):
                        gb.logging.info("Sending %s(%d)" %
                                      (wthr.get_weather_msg_str(wthr_msgType),
                                       wthr_msgType))
                wthr_q_out.put(wthrInfo)
                not_sent = False

            gb.time.sleep(DB_SLEEP)

        gb.logging.info("Exiting %s" % (self.name))
