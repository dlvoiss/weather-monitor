import gb

import db
import co
import wv
import rg

CO_SLEEP = 1

REQ_SZ = 50
REQS_AVG1 = [-1.0] * REQ_SZ  # mph
REQS_SDEV1 = [-1.0] * REQ_SZ  # mph
REQS_AVG5 = [-1.0] * REQ_SZ  # mph
REQS_SDEV5 = [-1.0] * REQ_SZ  # mph
REQS_DIR = [0] * REQ_SZ      # 1-8
REQS_RAIN = [-1.0] * REQ_SZ  # inches
REQS_DUMP = [0] * REQ_SZ     # count of bucket dumps
REQS_TIME = [0.0] * REQ_SZ   # epoch time
REQS_MPH = [0.0] * REQ_SZ   # windspeed in mph (not averaged)

#######################################################################
#
# Coordinator Thread
#
#######################################################################
class CoordinatorThread(gb.threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        gb.threading.Thread.__init__(self, group=group, target=target, name=name)
        gb.logging.info("Setting up Coordinator thread")
        self.args = args
        self.kwargs = kwargs
        self.name = name

    def request_wind_dir_rain(self, wv_q_out, rg_q_out, e_tm, req_id):
        msg=[]
        msgType = wv.WV_GET_DIRECTION
        msg.append(msgType)
        msg.append(e_tm)
        msg.append(req_id)
        if (gb.DIAG_LEVEL & gb.WIND_DIR_MSG):
            gb.logging.info("Sending %s(%d)" %
                            (wv.get_wv_msg_str(msgType), msgType))
        wv_q_out.put(msg)

        msg=[]
        msgType = rg.RG_GET_RAINFALL
        msg.append(msgType)
        msg.append(e_tm)
        msg.append(req_id)
        if (gb.DIAG_LEVEL & gb.RAIN_MSG):
            gb.logging.info("Sending %s(%d)" %
                            (rg.get_rg_msg_str(msgType), msgType))
        rg_q_out.put(msg)

    def process_winddir(self, msg):
        msgType = msg[0]
        if (gb.DIAG_LEVEL & gb.WIND_DIR_MSG or
            gb.DIAG_LEVEL & gb.WIND_DIR_DETAIL):
            gb.logging.info("Processing %s(%d)" %
                            (co.get_co_msg_str(msgType), msgType))
        e_tm = msg[1]
        req_id = msg[2]
        winddir_int = msg[3]
        REQS_DIR[req_id] = winddir_int

    def process_rainfall(self, msg):

        msgType = msg[0]
        e_tm = msg[1]
        req_id = msg[2]
        dump_cnt = msg[3] # number of times bucket dump occurred
        rainfall = msg[4]

        if (gb.DIAG_LEVEL & gb.RAIN_MSG):
            gb.logging.info("Processing %s(%d): rainfall %0.2f" %
                            (co.get_co_msg_str(msgType), msgType, rainfall))

        REQS_DUMP[req_id] = dump_cnt
        REQS_RAIN[req_id] = rainfall

    def send_gust_to_db(self, db_q, tm, avg5, mph, interval):
        msg = []
        msgType = db.DB_GUST
        msg.append(msgType)
        msg.append(tm)
        msg.append(avg5)
        msg.append(mph)
        msg.append(interval)
        if (gb.DIAG_LEVEL & gb.DB_MSG):
            gb.logging.info("Sending %s(%d)" %
                            (db.get_db_msg_str(msgType), msgType))
        db_q.put(msg)

    def process_gust(self, db_q, msg):
        msgType        = msg[0]
        gust_tm        = msg[1]
        wind_avg       = msg[2]
        gust_mph       = msg[3]
        gust_intervals = msg[4]
        if (gb.DIAG_LEVEL & gb.GUSTS_MSG):
            gb.logging.info("Processing %s(%d)" %
                                (co.get_co_msg_str(msgType), msgType))
            gb.logging.info("Wind Avg: %0.1f mph; GUST %s: %0.1f mph %d intervals" %
                            (wind_avg, gust_tm, gust_mph, gust_intervals))
        self.send_gust_to_db(db_q, gust_tm, wind_avg, gust_mph, gust_intervals)


    def send_reading_to_db(self, db_q, tm_str, wavg1, wsdev1, wavg5, wsdev5,
                           windspeed, wdir, wdir_str, rdump_cnt, rtally):
        msg = []
        msgType = db.DB_READING
        msg.append(msgType)
        msg.append(tm_str)
        msg.append(wavg1)
        msg.append(wsdev1)
        msg.append(wavg5)
        msg.append(wsdev5)
        msg.append(windspeed)
        msg.append(wdir)
        msg.append(wdir_str)
        msg.append(rdump_cnt)
        msg.append(rtally)
        if (gb.DIAG_LEVEL & gb.DB_MSG):
            gb.logging.info("Sending %s(%d)" %
                            (db.get_db_msg_str(msgType), msgType))
        db_q.put(msg)

    def send_db_test_msg(self, db_q):
        msg = []
        msgType = db.DB_TEST
        msg.append(msgType)
        gb.logging.info("Sending %s(%d)" %
                        (db.get_db_msg_str(msgType), msgType))
        db_q.put(msg)

    def process_windmax(self, db_q, msgType, msg):
        err = False
        if (msgType == co.CO_MP_MAX_1_HOUR):
            msg[0] = db.DB_MAX_1_HOUR
        elif (msgType == co.CO_MP_MAX_TODAY):
            msg[0] = db.DB_MAX_TODAY
        else:
            gb.logging.error("Invalid CO message type: %d" % (msgType))
            err = True

        if (err == False):
            if (gb.DIAG_LEVEL & gb.DB_MSG):
                gb.logging.info("Sending %s(%d)" %
                                (db.get_db_msg_str(msg[0]), msg[0]))
            db_q.put(msg)

    #############################################################
    # Database run function
    #############################################################
    def run(self):
        co_q_in = self.args[0]
        co_mp_q_in = self.args[1]
        wv_q_out = self.args[2]
        rg_q_out = self.args[3]
        db_q_out = self.args[4]
        end_event = self.args[5]

        gb.logging.info("Running %s" % (self.name))
        gb.logging.debug(self.args)

        avg_windspeed = 0.0
        wind_dir = wv.NORTH
        rain = 0.0
        req_id = 0

        old_wind_avg = -1.0
        old_windspeed = -1.0
        old_wind_dir = 0
        old_rain_tally = -1.0
        old_dump_cnt = -1
        
        ###################################################
        # Coordiantor main loop
        ###################################################
        while not end_event.isSet():

            msgType = ""
            while not co_q_in.empty() or not co_mp_q_in.empty():

                #-----------------------------
                # First check for a message from a thread
                # (Thread vs. process order is arbitrary)
                #-----------------------------
                if (not co_q_in.empty()):
                    msg = co_q_in.get()
                    msgType = msg[0]
                    if (gb.DIAG_LEVEL & gb.COOR_MSG and
                        gb.DIAG_LEVEL & gb.COOR_DETAIL):
                        gb.logging.info("%s:  Received %s(%d)" %
                            (self.name, co.get_co_msg_str(msgType), msgType))

                    if (msgType == co.CO_EXIT):
                        gb.logging.info("%s: Cleanup prior to exit" %
                                        (self.name))

                    elif (msgType == co.CO_WIND_DIR):
                        if (gb.DIAG_LEVEL & gb.WIND_DIR_MSG):
                            gb.logging.info("Processing %s(%d)" %
                                     (co.get_co_msg_str(msgType), msgType))
                        self.process_winddir(msg)

                    else:
                        gb.logging.error("Invalid CO message type: %d" %
                                         (msgType))
                        gb.logging.error(msg)

                #-----------------------------
                # Then check for messages from other processes
                #-----------------------------
                if (not co_mp_q_in.empty()):
                    msg = co_mp_q_in.get()
                    msgType = msg[0]
                    if (gb.DIAG_LEVEL & gb.WIND_AVG_MSG and
                        gb.DIAG_LEVEL & gb.WIND_AVG_DETAIL):
                        gb.logging.info("%s:  Received MP %s(%d)" %
                            (self.name, co.get_co_msg_str(msgType), msgType))

                    if (msgType == co.CO_MP_SHORT_WINDSPEED):
                        if (gb.DIAG_LEVEL & gb.WIND_AVG_MSG):
                            gb.logging.info("Received MP %s(%d)" %
                                (co.get_co_msg_str(msgType), msgType))
                        req_id = req_id + 1
                        if (req_id >= REQ_SZ):
                            req_id = 0
                        epoch_time = gb.time.time()
                        avg1_mph = msg[1]
                        sdev1_mph = msg[2]
                        avg5_mph = msg[3]
                        sdev5_mph = msg[4]
                        windspeed = msg[5]
                        REQS_AVG1[req_id] = avg1_mph
                        REQS_SDEV1[req_id] = sdev1_mph
                        REQS_AVG5[req_id] = avg5_mph
                        REQS_SDEV5[req_id] = sdev5_mph
                        REQS_TIME[req_id] = epoch_time
                        REQS_DIR[req_id] = 0
                        REQS_RAIN[req_id] = -1.0
                        REQS_DUMP[req_id] = -1
                        REQS_MPH[req_id] = windspeed
                        self.request_wind_dir_rain(wv_q_out, rg_q_out,
                                                   epoch_time, req_id)

                    elif (msgType == co.CO_MP_LONG_WINDSPEED):
                        if (gb.DIAG_LEVEL & gb.WIND_AVG_MSG):
                            gb.logging.info("Received %s(%d)" %
                                (co.get_co_msg_str(msgType), msgType))

                    elif (msgType == co.CO_MP_RAINFALL):
                        if (gb.DIAG_LEVEL & gb.RAIN_MSG):
                            gb.logging.info("Received %s(%d)" %
                                (co.get_co_msg_str(msgType), msgType))
                        self.process_rainfall(msg)

                    elif (msgType == co.CO_MP_GUST):
                        if (gb.DIAG_LEVEL & gb.GUSTS_MSG):
                            gb.logging.info("Received %s(%d)" %
                                (co.get_co_msg_str(msgType), msgType))
                        self.process_gust(db_q_out, msg)

                    elif (msgType == co.CO_MP_MAX_1_HOUR):
                        if (gb.DIAG_LEVEL & gb.WIND_MAX):
                            gb.logging.info("Processing %s(%d)" %
                                     (co.get_co_msg_str(msgType), msgType))
                        self.process_windmax(db_q_out, msgType, msg)

                    elif (msgType == co.CO_MP_MAX_TODAY):
                        if (gb.DIAG_LEVEL & gb.WIND_MAX):
                            gb.logging.info("Processing %s(%d)" %
                                     (co.get_co_msg_str(msgType), msgType))
                        self.process_windmax(db_q_out, msgType, msg)

                    else:
                        gb.logging.error("Invalid CO MP message type: %d" %
                            (msgType))
                        gb.logging.error(msg)

                gb.time.sleep(0.5)

            # Once all thread andd process messages have been handled,
            # check for any completed requests.  The requests have an
            # ID and timestamp associated with them, and gather up the
            # wind speed, wind vane, and rain gauge information from
            # approximately the same timeframe

            if (msgType != co.CO_EXIT):

                #self.send_db_test_msg(db_q_out)

                #-----------------------------
                # Count uncompleted weather requests
                #-----------------------------
                inprogress = 0
                for ix in range(REQ_SZ):
                    if (REQS_AVG1[ix] != -1.0):
                        inprogress = inprogress + 1 

                #-----------------------------
                # Check for any completed weather requests where the
                # data differs from the last data written to the DB
                #-----------------------------
                for ix in range(REQ_SZ):
                    if ((REQS_AVG1[ix] != -1.0) and
                        (REQS_AVG5[ix] != -1.0) and
                        (REQS_DIR[ix] != wv.INVALID) and
                        (REQS_RAIN[ix] != -1.0)):

                        wind_avg1 = REQS_AVG1[ix]
                        wind_sdev1 = REQS_SDEV1[ix]

                        wind_avg5 = REQS_AVG5[ix]
                        wind_sdev5 = REQS_SDEV5[ix]

                        rain_tally = REQS_RAIN[ix]
                        #if (rain_tally != 0.0):
                        #    rain_tally = rain_tally + 0.005
                        rain_dump_cnt = REQS_DUMP[ix]

                        wind_dir = REQS_DIR[ix]
                        wind_dir_str = wv.wind_dir_int_to_str(wind_dir)

                        date_seconds = str(
                                    gb.datetime.fromtimestamp(REQS_TIME[ix]))
                        date_seconds = gb.get_date_with_seconds(date_seconds)

                        #-----------------------------
                        # Log values only if one or more changed
                        # from prior readings
                        #-----------------------------
                        if ((old_wind_avg != wind_avg1) or
                            (old_windspeed != windspeed) or
                            (old_rain_tally != rain_tally) or
                            (REQS_DIR[ix] != old_wind_dir) or
                            (rain_dump_cnt != old_dump_cnt)):
                            gb.logging.debug(
                                "OLD: %0.1f MPH, Dir %d(%d), %0.2f in" %
                                (old_wind_avg, old_wind_dir, REQS_DIR[ix],
                                 old_rain_tally))
                            if (gb.DIAG_LEVEL & gb.COOR):
                                gb.logging.info(
                                    "%d: %s: %0.1f MPH, %s; %0.2f in, cnt %d" %
                                    (ix, date_seconds,
                                     wind_avg1,
                                     wind_dir_str,
                                     rain_tally, inprogress))

                            self.send_reading_to_db(db_q_out, date_seconds,
                                                    wind_avg1, wind_sdev1,
                                                    wind_avg5, wind_sdev5,
                                                    windspeed,
                                                    wind_dir, wind_dir_str,
                                                    rain_dump_cnt, rain_tally)

                            old_wind_avg = wind_avg1
                            old_windspeed = windspeed
                            old_wind_dir = REQS_DIR[ix]
                            old_rain_tally = rain_tally
                            old_dump_cnt = rain_dump_cnt

                        REQS_TIME[ix] = 0.0
                        REQS_AVG1[ix] = -1.0
                        REQS_SDEV1[ix] = -1.0
                        REQS_AVG5[ix] = -1.0
                        REQS_SDEV5[ix] = -1.0
                        REQS_DIR[ix] = 0
                        REQS_RAIN[ix] = -1.0
                        REQS_DUMP[ix] = -1
                        REQS_MPH[ix] = -1.0

                gb.time.sleep(CO_SLEEP)

        gb.logging.info("Exiting %s" % (self.name))
