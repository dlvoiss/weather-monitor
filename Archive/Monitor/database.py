import gb
import db
import fan
import wthr
import avg

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

temp_min_mo_update_sql = """UPDATE monthdata SET mintempf=%s, mintempfts=%s WHERE id=%s"""
temp_max_mo_update_sql = """UPDATE monthdata SET maxtempf=%s, maxtempfts=%s WHERE id=%s"""
humid_min_mo_update_sql = """UPDATE monthdata SET minhum=%s, minhumts=%s WHERE id=%s"""
humid_max_mo_update_sql = """UPDATE monthdata SET maxhum=%s, maxhumts=%s WHERE id=%s"""
baro_min_mo_update_sql = """UPDATE monthdata SET lowmB=%s, lowmBts=%s WHERE id=%s"""
baro_max_mo_update_sql = """UPDATE monthdata SET highmB=%s, highmBts=%s WHERE id=%s"""

temp_min_all_time_update_sql = """UPDATE alltimedata SET mintempf=%s, mintempfts=%s WHERE id=%s"""
temp_max_all_time_update_sql = """UPDATE alltimedata SET maxtempf=%s, maxtempfts=%s WHERE id=%s"""
humid_min_all_time_update_sql = """UPDATE alltimedata SET minhum=%s, minhumts=%s WHERE id=%s"""
humid_max_all_time_update_sql = """UPDATE alltimedata SET maxhum=%s, maxhumts=%s WHERE id=%s"""
baro_min_all_time_update_sql = """UPDATE alltimedata SET lowmB=%s, lowmBts=%s WHERE id=%s"""
baro_max_all_time_update_sql = """UPDATE alltimedata SET highmB=%s, highmBts=%s WHERE id=%s"""

SQL_UPDT_1DAY = []
SQL_UPDT_30DAY = []
SQL_UPDT_CUR_MO = []
SQL_UPDT_ALL_TIME = []

MAX_1DAY     = [0.0] * 6
MAX_1DAY_TS  = [gb.DFLT_TIME] * 6
MAX_30DAY    = [0.0] * 6
MAX_30DAY_TS = [gb.DFLT_TIME] * 6

DAYS1 = 1
DAYS30 = 30

#######################################################################
#
# Database Thread
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

    #------------------------------------
    def add_start_record(self):

        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

        db_id = 1

        try:
            add_start = ("REPLACE INTO windrain "
                         "(recordType, tmstamp, dbid) "
                         "VALUES (%s, %s, %s)")
            data_add = ('START', tm_str, db_id)

            self.db_cursor.execute(add_start, data_add)
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        gb.logging.info("Added START record to windrain table")

    #------------------------------------
    def add_stop_record(self):

        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

        db_id = 2
        try:
            add_stop = ("REPLACE INTO windrain "
                         "(recordType, tmstamp, dbid) "
                         "VALUES (%s, %s, %s)")
            data_add = ('STOP', tm_str, db_id)

            self.db_cursor.execute(add_stop, data_add)
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        gb.logging.info("Added STOP record to windrain table")

    #------------------------------------
    def process_weather_reading(self, msgType, msg):

        tm_str    = msg[1]
        wavg1     = msg[2]
        wsdev1    = msg[3]
        wavg5     = msg[4]
        wsdev5    = msg[5]
        windspeed = msg[6]
        wdir      = msg[7]
        wdir_str  = msg[8]
        rdump     = msg[9]
        rtally    = msg[10]

        if (gb.DIAG_LEVEL & gb.DB):
            gb.logging.info("Running process_weather_reading(): tmstamp %s" %
                            (tm_str))
            gb.logging.info("tm_str: %s %.1f mph %0.2f inches cnt %d" %
                            (tm_str, wavg1, rtally, rdump))

        db_id = 3

        try:
            add_reading = ("REPLACE INTO windrain "
                         "(recordType, tmstamp, dbid, windavg1, windsdev1, windavg5, windsdev5, windspeed, dir, winddir, rainfall, rainfall_counter) "
                         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_add = ('AVG', tm_str, db_id, wavg1, wsdev1, wavg5, wsdev5, windspeed, wdir, wdir_str, rtally, rdump)

            self.db_cursor.execute(add_reading, data_add)
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 4
        try:
            self.db_cursor.execute("""UPDATE current_stats SET windavg1=%s, windavg5=%s, windspeed=%s, wind_dir=%s, wind_dir_str=%s, rainfall_today=%s, rainfall_counter=%s WHERE id=1""", (wavg1, wavg5, windspeed, wdir, wdir_str, rtally, rdump))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_gust(self, msgType, msg):
        tm_str = msg[1]
        avg5 = msg[2]
        mph = msg[3]
        intervals = msg[4]

        if (gb.DIAG_LEVEL & gb.DB):
            gb.logging.info("Running process_gust(): tmstamp %s" %
                            (tm_str))
            gb.logging.info("tm_str: %s avg %.1f, gust %0.1f mph intervals %d" %
                            (tm_str, avg5, mph, intervals))
        db_id = 5
        try:
            db_sql = ("REPLACE INTO windrain "
                      "(recordType, tmstamp, dbid, windavg5, windgust, gust_interval) "
                      "VALUES (%s, %s, %s, %s, %s, %s)")
            data_add = ('GUST', tm_str, db_id, avg5, mph, intervals)
            self.db_cursor.execute(db_sql, data_add)
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 6
        try:
            self.db_cursor.execute("""UPDATE current_stats SET gust_tm=%s, gust=%s, gust_intervals=%s WHERE id=1""", (tm_str, mph, intervals))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_max_1_hour(self, msgType, msg):
        max_tm = str(msg[1])
        mph = msg[2]

        db_id = 7
        try:
            self.db_cursor.execute("""UPDATE current_stats SET windmax1hour_tm=%s, windmax1hour=%s WHERE id=1""", (max_tm, mph))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_max_today(self, msgType, msg):
        max_tm = str(msg[1])
        mph = msg[2]

        db_id = 8
        try:
            self.db_cursor.execute("""UPDATE current_stats SET windmaxtoday_tm=%s, windmaxtoday=%s WHERE id=1""", (max_tm, mph))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

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
    #def db_update_remote(self, db_data):
    #    gb.logging.debug("db_update_remote")
    #    #print(db_data)
    #    tm_str = db_data[1]
    #    pressure_hPa = db_data[2]
    #    temp_f = db_data[3]
    #    temp_c = db_data[4]
    #    humidity = db_data[5]
    #    wind_speed = db_data[6]
    #    wind_degrees = db_data[7]
    #    wind_gusts = db_data[8]
    #    sunrise = db_data[9]
    #    sunset = db_data[10]

    #    db_id = 11
    #    try:
    #        if (gb.DIAG_LEVEL & 0x10):
    #            gb.logging.info("REPLACE INTO readings; db_id: %d" % (db_id))

    #        add_remote_weather = ("REPLACE INTO readings "
    #                             "(recordType, tmstamp, baromB, "
    #                             "tempdhtf, tempdhtc, humidity) "
    #                             "VALUES (%s, %s, %s, %s, %s, %s)")
    #        data_add = ('REMOTE', tm_str, pressure_hPa, temp_f, temp_c, humidity)

    #        self.db_cursor.execute(add_remote_weather, data_add)
    #        self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x20):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

    #    except mariadb.Error as e:
    #        gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #    db_id = 61
    #    try:
    #        if (gb.DIAG_LEVEL & 0x10):
    #            gb.logging.info("REPLACE INTO readings; db_id: %d" % (db_id))

    #        add_remote_weather = ("REPLACE INTO rmt_readings "
    #                             "(tmstamp, baromBrmt, "
    #                             "temprmtf, humidityrmt, windspeed, winddir, "
    #                             "windgust, sunrise, sunset) "
    #                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    #        data_add = (tm_str, pressure_hPa, temp_f, humidity, wind_speed, wind_degrees, wind_gusts, sunrise, sunset)

    #        self.db_cursor.execute(add_remote_weather, data_add)
    #        self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x20):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

    #    except mariadb.Error as e:
    #        gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #############################################################
    # Initialize in-memory tallies and average temperatures for a month
    # DB_INIT_AVG_TEMP
    #############################################################
    def db_get_avg_temp(self, db_data, avg_q_out):

        global DB_SUCCESS
        global DB_SELECT

        gb.logging.info("Processing db_get_avg_temp: DB_INIT_AVG_TEMP")

        month = db_data[1]

        db_error = DB_SUCCESS;

        db_id = 102
        err_resp = -db_id

        sql_select = "SELECT daytally,avgdaytimetempf,nighttally,avgnighttimetempf from monthavg where id={0}".format(month)
        gb.logging.debug("averages SQL: %s" % (sql_select))
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
                print(select_data[0],select_data[1])
                print(select_data[2],select_data[3])

            daytally = select_data[0]
            dayavg = select_data[1]
            nighttally = select_data[2]
            nightavg = select_data[3]

        else:
            gb.logging.info("Failed to get day/night running averages")
            return

        db_id = 105
        err_resp = -db_id

        calc_id = 13
        sql_select = "SELECT avghightally,avghightempf,avglowtally,avglowtempf from monthavg where id={0}".format(month)
        gb.logging.debug("averages SQL: %s" % (sql_select))
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

            hightally = select_data[0]
            highavg = select_data[1]
            lowtally = select_data[2]
            lowavg = select_data[3]

            avg_msgType = avg.AVG_INIT
            avgInfo = []
            avgInfo.append(avg_msgType)
            avgInfo.append(month)
            avgInfo.append(daytally)
            avgInfo.append(dayavg)
            avgInfo.append(nighttally)
            avgInfo.append(nightavg)
            avgInfo.append(hightally)
            avgInfo.append(highavg)
            avgInfo.append(lowtally)
            avgInfo.append(lowavg)
            if (gb.DIAG_LEVEL & 0x400):
                gb.logging.info("Sending %s(%d)" %
                             (avg.get_avg_msg_str(avg_msgType),
                              avg_msgType))
            avg_q_out.put(avgInfo)

        else:
            gb.logging.info("Failed to get day high/low running averages")
            return

    #############################################################
    # Update tally and average temperature for a month
    # DB_AVG_TEMP
    #############################################################
    def db_get_cur_mo(self, db_data, wthr_q_out):

        global DB_SUCCESS
        global DB_SELECT

        month = db_data[1]

        db_error = DB_SUCCESS;

        db_id = 103
        err_resp = -db_id

        sql_select = "SELECT mintempfts,mintempf,maxtempfts,maxtempf,minhumts,minhum,maxhumts,maxhum,lowmBts,lowmB,highmBts,highmB,month from monthdata where id={0}".format(month)
        if (gb.DIAG_LEVEL & 0x1000):
            gb.logging.info("cur mo SQL: %s" % (sql_select))
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

            mintempfts = select_data[0]
            mintempf = select_data[1]
            maxtempfts = select_data[2]
            maxtempf = select_data[3]
            minhumts = select_data[4]
            minhum = select_data[5]
            maxhumts = select_data[6]
            maxhum = select_data[7]
            lowmBts = select_data[8]
            lowmB = select_data[9]
            highmBts = select_data[10]
            highmB = select_data[11]
            month_str = select_data[12]

            wthr_msgType = wthr.WTHR_INIT_CUR_MO
            wthrInfo = []
            wthrInfo.append(wthr_msgType)
            wthrInfo.append(month)
            wthrInfo.append(month_str)
            wthrInfo.append(mintempfts)
            wthrInfo.append(mintempf)
            wthrInfo.append(maxtempfts)
            wthrInfo.append(maxtempf)
            wthrInfo.append(minhumts)
            wthrInfo.append(minhum)
            wthrInfo.append(maxhumts)
            wthrInfo.append(maxhum)
            wthrInfo.append(lowmBts)
            wthrInfo.append(lowmB)
            wthrInfo.append(highmBts)
            wthrInfo.append(highmB)
            if (gb.DIAG_LEVEL & 0x400):
                gb.logging.info("Sending %s(%d)" %
                             (wthr.get_weather_msg_str(wthr_msgType),
                              wthr_msgType))
            wthr_q_out.put(wthrInfo)
        else:
            gb.logging.error("Error retrieving current month data")

    #############################################################
    # Update tally and average temperature for a month
    # DB_AVG_TEMP
    #############################################################
    def db_get_all_time(self, db_data, wthr_q_out):

        global DB_SUCCESS
        global DB_SELECT

        month = db_data[1]

        db_error = DB_SUCCESS;

        db_id = 104
        err_resp = -db_id

        sql_select = "SELECT mintempfts,mintempf,maxtempfts,maxtempf,minhumts,minhum,maxhumts,maxhum,lowmBts,lowmB,highmBts,highmB,month from alltimedata where id={0}".format(month)
        if (gb.DIAG_LEVEL & 0x2000):
            gb.logging.info("all-time SQL: %s" % (sql_select))
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

            mintempfts = select_data[0]
            mintempf = select_data[1]
            maxtempfts = select_data[2]
            maxtempf = select_data[3]
            minhumts = select_data[4]
            minhum = select_data[5]
            maxhumts = select_data[6]
            maxhum = select_data[7]
            lowmBts = select_data[8]
            lowmB = select_data[9]
            highmBts = select_data[10]
            highmB = select_data[11]
            month_str = select_data[12]

            wthr_msgType = wthr.WTHR_INIT_ALL_TIME
            wthrInfo = []
            wthrInfo.append(wthr_msgType)
            wthrInfo.append(month)
            wthrInfo.append(month_str)
            wthrInfo.append(mintempfts)
            wthrInfo.append(mintempf)
            wthrInfo.append(maxtempfts)
            wthrInfo.append(maxtempf)
            wthrInfo.append(minhumts)
            wthrInfo.append(minhum)
            wthrInfo.append(maxhumts)
            wthrInfo.append(maxhum)
            wthrInfo.append(lowmBts)
            wthrInfo.append(lowmB)
            wthrInfo.append(highmBts)
            wthrInfo.append(highmB)
            if (gb.DIAG_LEVEL & 0x400):
                gb.logging.info("Sending %s(%d)" %
                             (wthr.get_weather_msg_str(wthr_msgType),
                              wthr_msgType))
            wthr_q_out.put(wthrInfo)
        else:
            gb.logging.error("Error retrieving current month data")

    #############################################################
    # Update tally and average temperature for a month
    # DB_AVG_TEMP
    #############################################################
    def db_update_avg_temp(self, db_data):

        global DB_UPDATE
        global DB_COMMIT

        cur_month = db_data[1]  # primary key
        dayornight = db_data[2]
        tally = db_data[3]
        running_avg = db_data[4]

        if (dayornight == 1):
            # Update daytime temperture running average
            db_id = 100
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE monthavg; db_id: %d" % (db_id))

                self.db_cursor.execute("""UPDATE monthavg SET daytally=%s, avgdaytimetempf=%s WHERE id=%s""", (tally, running_avg, cur_month))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        else:
            # Update nighttime temperture running average
            db_id = 101
            try:
                if (gb.DIAG_LEVEL & 0x10):
                    gb.logging.info("UPDATE monthavg; db_id: %d" % (db_id))

                self.db_cursor.execute("""UPDATE monthavg SET nighttally=%s, avgnighttimetempf=%s WHERE id=%s""", (tally, running_avg, cur_month))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
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

    #-----------------------------------------
    # Update current stats, 30-day stats and today stats
    # based on latest readings sent to DB via DB_CUR_STATS
    #-----------------------------------------
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
        gb.logging.info("date_from_reading SQL: %s" % (sql_var))
        #gb.logging.debug("date_from_reading SQL: %s" % (sql_var))

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
            gb.logging.error("%s" % sql_var)

        return(resp)

    #-----------------------------------------
    # As part of startup, find min/max readings for the last 1-day in
    # the readings table, and use the information to update min/max
    # information in the currentreadings table
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
    # As part of startup, find min/max readings for the last 30 days in
    # the readings table, and use the information to update min/max
    # information in the readings30 table
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

    #-----------------------------------------
    # Get sunrise and sunset times from sun table
    #-----------------------------------------
    def db_get_suntimes(self, db_data, avg_q_out):

        date = db_data[1]

        db_id = 300
        err_resp = -db_id
        db_error = DB_SUCCESS;

        sql_select = 'SELECT sunrise, sunset from sun where dt="{0}"'.format(date)
        gb.logging.info("averages SQL: %s" % (sql_select))

        try:
            select = self.db_cursor.execute(sql_select)
            select_data = self.db_cursor.fetchone()

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_SELECT = DB_SELECT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))
            db_error = err_resp

        if (db_error == DB_SUCCESS):
            elements_in_select_data = len(select_data)
            gb.logging.info("elements_in_select_data: %d" %
                                (elements_in_select_data))
            print(select_data[0],select_data[1])

            sunrise = select_data[0]
            sunset = select_data[1]

            avg_msgType = avg.AVG_SUNTIMES
            avgInfo = []
            avgInfo.append(avg_msgType)
            avgInfo.append(date)
            avgInfo.append(sunrise)
            avgInfo.append(sunset)
            gb.logging.info("Sending %s(%d)" %
                             (avg.get_avg_msg_str(avg_msgType),
                              avg_msgType))
            avg_q_out.put(avgInfo)

        else:
            gb.logging.info("Failed to get sunrise/sunset times")
            return

        if (db_error == DB_SUCCESS):
            db_id = 301
            try:
                self.db_cursor.execute("""UPDATE current_stats SET dt=%s, sunrise=%s, sunset=%s WHERE id=1""", (date, sunrise, sunset))
                self.db_cursor.execute("COMMIT")

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #-----------------------------------------
    # Update current month or all-time minimum or maximum reading
    #-----------------------------------------
    def db_update_mo_readings(self, db_data):

        global SQL_UPDT_CUR_MO
        global SQL_UPDT_ALL_TIME

        db_error = DB_SUCCESS;

        min_max = db_data[1]
        field = db_data[2]
        cur_or_all = db_data[3]
        month = db_data[4]
        reading_ts = db_data[5]
        reading = db_data[6]

        multiplier = 1 # db.DB_CUR_MO
        if (min_max == db.DB_MAX):
            multiplier = 2

        field_ix = 1 # db.DB_TEMPF
        if (field == db.DB_HUM):
            field_ix = 2
        elif (field == db.DB_MB):
            field_ix = 3

        # Treated as 2x3 array
        sql_selector = ((field_ix - 1) * 2) + multiplier - 1

        db_id = 200 + sql_selector
        try:
            input_data = (reading, reading_ts, month)
            if (cur_or_all == db.DB_CUR_MO):
                if (gb.DIAG_LEVEL & 0x1000):
                    gb.logging.info("CUR MO SQL[%d]: %s" % 
                                    (sql_selector,
                                     SQL_UPDT_CUR_MO[sql_selector]))
                    gb.logging.info("INPUT[%d]: %.1f, %s, %d" % 
                                    (sql_selector, reading, reading_ts, month))
                self.db_cursor.execute(SQL_UPDT_CUR_MO[sql_selector],
                                       input_data)
            else:
                if (gb.DIAG_LEVEL & 0x2000):
                    gb.logging.info("ALL TIME SQL[%d]: %s" % 
                                    (sql_selector,
                                     SQL_UPDT_ALL_TIME[sql_selector]))
                    gb.logging.info("INPUT[%d]: %.1f, %s, %d" % 
                                    (sql_selector, reading, reading_ts, month))
                self.db_cursor.execute(SQL_UPDT_ALL_TIME[sql_selector],
                                       input_data)
            self_db_conn.commit()

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #-----------------------------------------
    # Transfer daily high/low average to prior month, restart
    # running averages for new month.  Running averages are kept
    # in row (month) 13 and copied to the correct month at the
    # start of a new month as part of this reset function
    #-----------------------------------------
    def db_reset_hi_lo_avg(self, db_data):

        month = db_data[1]
        gb.logging.info("db_reset_hi_lo_avg(): month %d" % (month))

        global DB_SUCCESS

        if (month == 0):
            # month will be zero the first time this function is
            # called after program startup
            return

        db_id = 220
        err_resp = -db_id
        try:
            gb.logging.info("SELECT month hi/low avg as part of reset; db_id: %d" % (db_id))
            select = self.db_cursor.execute('SELECT avghightempf, avglowtempf from monthavg where id=13')
            select_data = self.db_cursor.fetchone()

            high_avg = select_data[0]
            low_avg = select_data[1]
            err_resp = DB_SUCCESS

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_SELECT = DB_SELECT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))
            db_error = err_resp

        if (err_resp == DB_SUCCESS):

            db_id = 221
            try:
                #if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("UPDATE month hi/low avg; db_id: %d" %
                                    (db_id))

                self.db_cursor.execute("""UPDATE monthavg SET avghightally=0, avghightempf=%s, avglowtally=0, avglowtempf=%s WHERE id=%s""", (high_avg, low_avg, month))
                self.db_cursor.execute("COMMIT")

                #if (gb.DIAG_LEVEL & 0x4):
                #    DB_UPDATE = DB_UPDATE + 1
                #    DB_COMMIT = DB_COMMIT + 1

            except mariadb.Error as e:
                gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))


    #-----------------------------------------
    # Update daily high/low running average in DB at end of day
    #-----------------------------------------
    def db_update_hi_lo_avg(self, db_data):

        gb.logging.info("db_update_hi_lo_avg()")
        month = db_data[1]
        month_str = db_data[2]
        min_ix = db_data[3]
        min_running_avg = db_data[4]
        max_ix = db_data[5]
        max_running_avg = db_data[6]

        # NOTE: This updates row 13 rather than current month row
        # Row 13 is transferred to current month at month's end

        db_id = 222
        try:
            if (gb.DIAG_LEVEL & 0x10):
                gb.logging.info("UPDATE monthavg running avg; db_id: %d" %
                                (db_id))
                gb.logging.info("Month: %s, min_ix: %.2f, min_running_avg: %.2f" %
                            (month_str, min_ix, min_running_avg))
                gb.logging.info("Month: %s, max_ix: %.2f, max_running_avg: %.2f" %
                            (month_str, max_ix, max_running_avg))

            self.db_cursor.execute("""UPDATE monthavg SET month=%s, avghightally=%s, avghightempf=%s, avglowtally=%s, avglowtempf=%s WHERE id=%s""", (month_str, max_ix, max_running_avg, min_ix, min_running_avg, 13))
            self.db_cursor.execute("COMMIT")

            #if (gb.DIAG_LEVEL & 0x4):
            #    DB_UPDATE = DB_UPDATE + 1
            #    DB_COMMIT = DB_COMMIT + 1

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

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
        avg_q_out = self.args[3]
        end_event = self.args[4]

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

        self.add_start_record()

        # temp min/max, humid min/max, mb min/max
        SQL_UPDT_1DAY.append(temp_min_update_sql)
        SQL_UPDT_1DAY.append(temp_max_update_sql)
        SQL_UPDT_1DAY.append(humid_min_update_sql)
        SQL_UPDT_1DAY.append(humid_max_update_sql)
        SQL_UPDT_1DAY.append(baro_min_update_sql)
        SQL_UPDT_1DAY.append(baro_max_update_sql)

        # temp min/max, humid min/max, mb min/max
        SQL_UPDT_30DAY.append(temp_min_30_update_sql)
        SQL_UPDT_30DAY.append(temp_max_30_update_sql)
        SQL_UPDT_30DAY.append(humid_min_30_update_sql)
        SQL_UPDT_30DAY.append(humid_max_30_update_sql)
        SQL_UPDT_30DAY.append(baro_min_30_update_sql)
        SQL_UPDT_30DAY.append(baro_max_30_update_sql)

        # all mins followed by all maxs
        SQL_UPDT_CUR_MO.append(temp_min_mo_update_sql)
        SQL_UPDT_CUR_MO.append(temp_max_mo_update_sql)
        SQL_UPDT_CUR_MO.append(humid_min_mo_update_sql)
        SQL_UPDT_CUR_MO.append(humid_max_mo_update_sql)
        SQL_UPDT_CUR_MO.append(baro_min_mo_update_sql)
        SQL_UPDT_CUR_MO.append(baro_max_mo_update_sql)

        # all mins followed by all maxs
        SQL_UPDT_ALL_TIME.append(temp_min_all_time_update_sql)
        SQL_UPDT_ALL_TIME.append(temp_max_all_time_update_sql)
        SQL_UPDT_ALL_TIME.append(humid_min_all_time_update_sql)
        SQL_UPDT_ALL_TIME.append(humid_max_all_time_update_sql)
        SQL_UPDT_ALL_TIME.append(baro_min_all_time_update_sql)
        SQL_UPDT_ALL_TIME.append(baro_max_all_time_update_sql)

        ###################################################
        # DB main loop: Part 1: INCOMING MESSAGES
        ###################################################
        while not end_event.isSet():

            #gb.logging.info("DB Part 1: Check for incoming DB msgs")

            while not db_q_in.empty():
                db_data = db_q_in.get()
                db_msgType = db_data[0]
                gb.logging.debug(db_data)

                gb.logging.debug("Recvd: %s(%d)" %
                                 (db.get_db_msg_str(db_msgType),db_msgType))

                if (db_msgType == db.DB_EXIT):
                    gb.logging.info("%s: Cleanup prior to exit" % (self.name))

                elif (db_msgType == db.DB_CUR_STATS):
                    self.db_current_stats(db_data)

                elif (db_msgType == db.DB_LOCAL_STATS):
                    self.db_update_local(db_data)

                #elif (db_msgType == db.DB_REMOTE_STATS):
                #    self.db_update_remote(db_data)

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

                elif (db_msgType == db.DB_SUNTIMES):
                    self.db_get_suntimes(db_data, avg_q_out)

                elif (db_msgType == db.DB_AVG_TEMP):
                    self.db_update_avg_temp(db_data)

                elif (db_msgType == db.DB_INIT_AVG_TEMP):
                    self.db_get_avg_temp(db_data, avg_q_out)

                elif (db_msgType == db.DB_INIT_CUR_MO):
                    self.db_get_cur_mo(db_data, wthr_q_out)

                elif (db_msgType == db.DB_INIT_ALL_TIME):
                    self.db_get_all_time(db_data, wthr_q_out)

                elif (db_msgType == db.DB_UPDATE_MO_DATA):
                    self.db_update_mo_readings(db_data)

                elif (db_msgType == db.DB_AVG_HI_LO_RESET):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("Recvd: %s(%d)" %
                                 (db.get_db_msg_str(db_msgType),db_msgType))
                    self.db_reset_hi_lo_avg(db_data)

                elif (db_msgType == db.DB_UPDATE_AVG_HI_LO):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("Recvd: %s(%d)" %
                                 (db.get_db_msg_str(db_msgType),db_msgType))
                        gb.logging.info(db_data)
                    self.db_update_hi_lo_avg(db_data)

                #elif (db_msgType == db.DB_AVG_HI_LO_GET):
                #    gb.logging.info("Recvd: %s(%d)" %
                #                 (db.get_db_msg_str(db_msgType),db_msgType))
                #    gb.logging.info(db_data)
                #    self.db_get_hi_lo_avg(db_data, avg_q_out)

                elif (db_msgType == db.DB_READING):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing wind/rain msg %s(%d)" %
                            (self.name, db.get_db_msg_str(db_msgType),
                             db_msgType))
                    self.process_weather_reading(db_msgType, db_data)

                elif (db_msgType == db.DB_GUST):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(db_msgType),
                             db_msgType))
                    self.process_gust(db_msgType, db_data)

                elif (db_msgType == db.DB_MAX_1_HOUR):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(db_msgType),
                             db_msgType))
                    self.process_max_1_hour(db_msgType, db_data)

                elif (db_msgType == db.DB_MAX_TODAY):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(db_msgType),
                             db_msgType))
                    self.process_max_today(db_msgType, db_data)

                elif (db_msgType == db.DB_TEST):
                    gb.logging.info("%s: Processing %s(%d)" %
                            (self.name, db.get_db_msg_str(db_msgType),
                             db_msgType))

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

        self.add_stop_record()
        gb.time.sleep(1)
        gb.logging.info("Exiting %s" % (self.name))
