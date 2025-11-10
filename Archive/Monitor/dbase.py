import gb
import db

# mariadb imports
import mariadb
import sys
from decimal import Decimal

DB_SELECT = 0
DB_UPDATE = 0
DB_COMMIT = 0

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

    #------------------------------------
    def add_start_record(self):

        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

        db_id = 1
        try:
            self.db_cursor.execute("REPLACE INTO windrain SET recordtype = ?, tmstamp = ?, dbid = ?",('START', tm_str, db_id))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def add_stop_record(self):

        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

        db_id = 2
        try:
            self.db_cursor.execute("REPLACE INTO windrain SET recordtype = ?, tmstamp = ?, dbid = ?",('STOP', tm_str, db_id))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_weather_reading(self, msgType, msg):

        tm_str = msg[1]
        wavg1 = msg[2]
        wavg5 = msg[3]
        wdir = msg[4]
        wdir_str = msg[5]
        rdump = msg[6]
        rtally = msg[7]

        if (gb.DIAG_LEVEL & gb.DB):
            gb.logging.info("Running process_weather_reading(): tmstamp %s" %
                            (tm_str))
            gb.logging.info("tm_str: %s %.1f mph %0.2f inches cnt %d" %
                            (tm_str, wavg1, rtally, rdump))

        db_id = 3
        try:
            self.db_cursor.execute("REPLACE INTO windrain SET recordtype = ?, tmstamp = ?, dbid = ?, windavg1 = ?, windavg5 = ?, dir = ?, winddir = ?, rainfall = ?, rainfall_counter = ?",('AVG', tm_str, db_id, wavg1, wavg5, wdir, wdir_str, rtally, rdump))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 4
        try:
            self.db_cursor.execute("UPDATE current_stats SET windavg1 = ?, windavg5 = ?, wind_dir = ?, wind_dir_str = ?, rainfall_today = ?, rainfall_counter = ?",(wavg1, wavg5, wdir, wdir_str, rtally, rdump))
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
            self.db_cursor.execute("REPLACE INTO windrain SET recordtype = ?, tmstamp = ?, dbid = ?, windavg5 = ?, windgust = ?, gust_interval = ?",('GUST', tm_str, db_id, avg5, mph, intervals))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

        db_id = 6
        try:
            self.db_cursor.execute("UPDATE current_stats SET gust_tm = ?, gust = ?, gust_intervals = ?",(tm_str, mph, intervals))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_max_1_hour(self, msgType, msg):
        max_tm = str(msg[1])
        mph = msg[2]

        db_id = 7
        try:
            self.db_cursor.execute("UPDATE current_stats SET windmax1hour_tm = ?, windmax1hour = ?",(max_tm, mph))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #------------------------------------
    def process_max_today(self, msgType, msg):
        max_tm = str(msg[1])
        mph = msg[2]

        db_id = 8
        try:
            self.db_cursor.execute("UPDATE current_stats SET windmaxtoday_tm = ?, windmaxtoday = ?",(max_tm, mph))
            self.db_cursor.execute("COMMIT")

        except mariadb.Error as e:
            gb.logging.error("MariaDB ERROR %d: %s" % (db_id, e))

    #############################################################
    # Database run function
    #############################################################
    def run(self):
        global DB_SELECT

        db_q_in = self.args[0]
        co_q_out = self.args[1]
        end_event = self.args[2]

        gb.logging.info("Running %s" % (self.name))
        gb.logging.debug(self.args)

        iter = 0
        DB_SLEEP = 1

        try:
            self_db_conn = mariadb.connect(
                user="root",
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

        ###################################################
        # DB main loop: Part 1: INCOMING MESSAGES
        ###################################################
        while not end_event.isSet():

            if (gb.DIAG_LEVEL & gb.DB_DETAIL):
                gb.logging.info("DB Part 1: Check for incoming DB msgs")

            while not db_q_in.empty():
                msg = db_q_in.get()
                msgType = msg[0]
                if (gb.DIAG_LEVEL & gb.DB_DETAIL):
                    gb.logging.info("%s:  Received %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))

                if (msgType == db.DB_EXIT):
                    gb.logging.info("%s: Cleanup prior to exit" % (self.name))

                elif (msgType == db.DB_READING):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing wind/rain msg %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))
                    self.process_weather_reading(msgType, msg)

                elif (msgType == db.DB_GUST):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))
                    self.process_gust(msgType, msg)

                elif (msgType == db.DB_MAX_1_HOUR):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))
                    self.process_max_1_hour(msgType, msg)

                elif (msgType == db.DB_MAX_TODAY):
                    if (gb.DIAG_LEVEL & gb.DB_MSG):
                        gb.logging.info("%s: Processing gust msg %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))
                    self.process_max_today(msgType, msg)

                elif (msgType == db.DB_TEST):
                    gb.logging.info("%s: Processing %s(%d)" %
                            (self.name, db.get_db_msg_str(msgType), msgType))

                else:
                    gb.logging.error("Invalid DB message type: %d" %
                        (msgType))
                    gb.logging.error(msg)

                gb.time.sleep(0.5)

            ###################################################
            # DB main loop: Part 2: Query DB for UI-based state changes
            ###################################################

            #if (gb.DIAG_LEVEL & gb.DB_DETAIL):
            #    gb.logging.info("DB Part 2: ")

            ##################
            # Check Database for change in CPU Fan
            # temperature on/off thresholds
            ##################

            #if (gb.DIAG_LEVEL & gb.DB_DETAIL):
            #    gb.logging.info("DB Part 3: Query DB CPU fan info")

            #if (gb.DIAG_LEVEL & gb.DB_DETAIL):
            #    gb.logging.info("%d: %s: Aliveness" % (iter, self.name))
            #    iter = iter + 1

            gb.time.sleep(DB_SLEEP)

        self.add_stop_record()
        gb.time.sleep(1)
        gb.logging.info("Exiting %s" % (self.name))
