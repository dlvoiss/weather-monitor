import gb
import db
import avg

CURRENT_MONTH = 0

DAYTIME_READING = True
NIGHTTIME_READING = False

SUNRISE_TODAY = gb.DFLT_TIME
SUNSET_TODAY = gb.DFLT_TIME

#######################################################################
#
# Monthly Data Thread
# Track monthly highs/lows and day-time/night-time averages
#
#######################################################################
class AveragingThread(gb.threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        gb.threading.Thread.__init__(self, group=group, target=target, name=name)
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.kill_received = False
        self.day_ix = 0.0
        self.day_average = 0.0
        self.night_ix = 0.0
        self.night_average = 0.0
        self.min_avg_ix = 0.0
        self.min_avg = 0.0
        self.max_avg_ix = 0.0
        self.max_avg = 0.0
        self.month = 0
        #self.init_1st = []
        return

    #----------------------------------------------
    # Set sunrise and sunset; these values are demarcation for
    # daytime and nighttime temperature averages
    #----------------------------------------------
    def avg_daily_suntimes(self, avg_data):

        global SUNRISE_TODAY
        global SUNSET_TODAY

        dt = str(avg_data[1])
        sr = str(avg_data[2])
        ss = str(avg_data[3])

        gb.logging.info("dt: %s, Sunrise: %s, Sunset: %s" % (dt, sr, ss))

        # Create string in format datatime can use
        dt_sr = dt + " " + sr
        dt_ss = dt + " " + ss

        gb.logging.info("Sunrise: %s" % (dt_sr))
        gb.logging.info("Sunset:  %s" % (dt_ss))

        # create datetime objects for sunrise and sunset
        format_data = "%Y-%m-%d %H:%M:%S"
        sunrise = gb.datetime.strptime(dt_sr, format_data)
        sunset = gb.datetime.strptime(dt_ss, format_data)
        
        if (sunrise != SUNRISE_TODAY or sunset != SUNSET_TODAY):

            SUNRISE_TODAY = sunrise
            SUNSET_TODAY = sunset

            gb.logging.info("Sunrise: %s; Sunset: %s" %
                                (str(SUNRISE_TODAY), str(SUNSET_TODAY)))

    #----------------------------------------------
    # Update running average data for current month:
    # - daylight running average
    # - nighttime running average
    # - daily high running average
    # - daily low running average
    #----------------------------------------------
    def init_running_avg(self, avg_data):

        global CURRENT_MONTH

        CURRENT_MONTH      = avg_data[1]
        self.day_ix        = float(avg_data[2])
        self.day_average   = float(avg_data[3])
        self.night_ix      = float(avg_data[4])
        self.night_average = float(avg_data[5])
        self.max_avg_ix    = float(avg_data[6])
        self.max_avg       = float(avg_data[7])
        self.min_avg_ix    = float(avg_data[8])
        self.min_avg       = float(avg_data[9])

        gb.logging.info("CURRENT_MONTH: %d" % (CURRENT_MONTH))
        gb.logging.info("day ix:   %.1f, running avg: %.1f" %
                        (self.day_ix, self.day_average))
        gb.logging.info("night ix: %.1f, running avg: %.1f" %
                        (self.night_ix, self.night_average))
        gb.logging.info("high ix:   %.1f, running avg: %.1f" %
                        (self.max_avg_ix, self.max_avg))
        gb.logging.info("low ix: %.1f, running avg: %.1f" %
                        (self.min_avg_ix, self.min_avg))

    #----------------------------------------------
    # Get running average for daytime (sunrise-to-sunset) and
    # nighttime (sunset-to-sunrise) temperatuer
    #----------------------------------------------
    def running_avg_f(self, dtime, ix, avg, dht_temp_f, day_reading):

        month_str = dtime.strftime("%B")
        time_of_day = "Nighttime"
        if (day_reading):
            time_of_day = "Daytime"

        avg = ((avg * (ix - 1.0)) + dht_temp_f) / ix
        if (gb.DIAG_LEVEL & 0x800):
            gb.logging.info("%s %s ix: %.1f, %.1f F running avg: %.1f" %
                            (month_str, time_of_day, ix, dht_temp_f, avg))
        return(avg)

    #----------------------------------------------
    # Determine month and daylight vs. nighttime condition, then
    # calculate indicated running average temperature for the current month
    #----------------------------------------------
    def monthly_running_avg_f(self, avg_data, db_q_out):

        global CURRENT_MONTH
        global NIGHTTIME_READING
        global DAYTTIME_READING

        dtime = avg_data[1]
        dht_temp_f = avg_data[2]

        if (CURRENT_MONTH != dtime.month):
            # Restart averages for new month
            CURRENT_MONTH = dtime.month
            self.day_ix = 0.0
            self.day_average = 0.0
            self.night_ix = 0.0
            self.night_average = 0.0
            if (gb.DIAG_LEVEL & 0x800):
                gb.logging.info("Current month set to: %d" % (CURRENT_MONTH))

        if (dtime < SUNRISE_TODAY or dtime > SUNSET_TODAY):

            # Night-time readings before sunrise after
            # SUNRISE_TODAY has been updated for the current day
            if (gb.DIAG_LEVEL & 0x800):
                gb.logging.debug("Pre sunrise or post sunset")
            self.night_ix = self.night_ix + 1.0
            self.night_average = self.running_avg_f(dtime, self.night_ix,
                                                    self.night_average,
                                                    dht_temp_f,
                                                    NIGHTTIME_READING)
            dayornight = 0 # Nighttime

            db_msgType = db.DB_AVG_TEMP
            dbInfo = []
            dbInfo.append(db_msgType)
            dbInfo.append(CURRENT_MONTH)
            dbInfo.append(dayornight)
            dbInfo.append(self.night_ix)
            dbInfo.append(self.night_average)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d)" %
                         (db.get_db_msg_str(db_msgType),db_msgType))
            db_q_out.put(dbInfo)


        elif (dtime <= SUNSET_TODAY):

            # Day-time reading greater than SUNRISE_TODAY but
            # less than SUNSET_TODAY
            if (gb.DIAG_LEVEL & 0x800):
                gb.logging.debug("Post sunrise, pre-sunset")
            self.day_ix = self.day_ix + 1.0
            self.day_average = self.running_avg_f(dtime, self.day_ix,
                                                  self.day_average,
                                                  dht_temp_f, DAYTIME_READING)
            dayornight = 1 # Daytime

            db_msgType = db.DB_AVG_TEMP
            dbInfo = []
            dbInfo.append(db_msgType)
            dbInfo.append(CURRENT_MONTH)
            dbInfo.append(dayornight)
            dbInfo.append(self.day_ix)
            dbInfo.append(self.day_average)
            if (gb.DIAG_LEVEL & 0x8):
                gb.logging.info("Sending %s(%d)" %
                         (db.get_db_msg_str(db_msgType),db_msgType))
            db_q_out.put(dbInfo)

    #----------------------------------------------
    # Update avg_min_max
    #----------------------------------------------
    def avg_min_max_tempf_update(self, month_str, db_q_out):

        #if (gb.DIAG_LEVEL & 0x800):
        gb.logging.info("avg_min_max_tempf_update(), month: %s" % (month_str))
        gb.logging.info("%s(%d): min ix: %.1f, %.1f; max ix: %.1f, %.1f" %
                            (month_str, self.month, self.min_avg_ix,
                             self.min_avg, self.max_avg_ix, self.max_avg))

        # Send month_str and min/max running avg to DB to store in row 13

        db_msgT = db.DB_UPDATE_AVG_HI_LO
        dbInfo2 = []
        dbInfo2.append(db_msgT)
        dbInfo2.append(self.month)
        dbInfo2.append(month_str)
        dbInfo2.append(self.min_avg_ix)
        dbInfo2.append(self.min_avg)
        dbInfo2.append(self.max_avg_ix)
        dbInfo2.append(self.max_avg)
        #if (gb.DIAG_LEVEL & 0x8):
        gb.logging.info("Sending %s(%d)" %
                        (db.get_db_msg_str(db_msgT),db_msgT))
        db_q_out.put(dbInfo2)
        gb.time.sleep(1)

    #----------------------------------------------
    # Calculate running average minimum/maximum temperatures
    # for current month
    #----------------------------------------------
    def avg_min_max_tempf(self, avg_data, db_q_out):

        min_temp = avg_data[1]
        max_temp = avg_data[2]
        month = avg_data[3]
        month_str = avg_data[4]

        # NOTE: self.month is 0 the first time this function is
        # called after startup.  For this case, initialized
        # counts and running averages from DB

        if (gb.DIAG_LEVEL & gb.TPH_CUR_MO_AVG):
            gb.logging.info("PRE: self.month: %d, month: %d" %
                            (self.month, month))
            gb.logging.info("PRE: self.min_avg_ix %.1f self.min_avg %.1f" %
                            (self.min_avg_ix, self.min_avg))
            gb.logging.info("PRE: self.max_avg_ix %.1f self.max_avg %.1f" %
                            (self.max_avg_ix, self.max_avg))

        # Month is 0 after initial startup... but in rare cases
        # xxx_avg_ix counts could also be 0 (if first day of month) or
        # startup in mid-month
        if (self.month == 0):
            gb.logging.info("self.month is 0")
            #self.get_current_avg_min_max(db_q_out)

            self.month = month
            #self.init_1st = avg_data
            if (self.min_avg_ix == 0 or self.max_avg_ix == 0):
                if (self.min_avg_ix == 0):
                    gb.logging.info("Also, self.min_avg_ix == 0")
                    self.min_avg_ix = 1.0
                    self.min_avg = 0.0
                if (self.max_avg_ix == 0):
                    gb.logging.info("Also, self.max_avg_ix == 0")
                    self.max_avg_ix = 1.0
                    self.max_avg = 0.0
            else:
                gb.logging.info("No change to min/max_avg_ix")

        elif (self.month != month):
            #if (gb.DIAG_LEVEL & 0x800):
            gb.logging.info("RESET TALLY & averages for monthly high/low temp")
            self.min_avg_ix = 1.0
            self.min_avg = 0.0
            self.max_avg_ix = 1.0
            self.max_avg = 0.0

            # Move row 13 running avg min/max in DB to current self.month,
            # then update self.month... send info to DB.  Also 0.0 out
            # DB for daily min/max running avg in row 13

            db_msgType = db.DB_AVG_HI_LO_RESET
            dbInfo = []
            dbInfo.append(db_msgType)
            dbInfo.append(self.month)
            #if (gb.DIAG_LEVEL & 0x8):
            gb.logging.info("Sending %s(%d)" %
                            (db.get_db_msg_str(db_msgType),db_msgType))
            db_q_out.put(dbInfo)
            gb.time.sleep(1)

            self.month = month

        else:
            self.min_avg_ix = self.min_avg_ix + 1.0
            self.max_avg_ix = self.max_avg_ix + 1.0


        self.min_avg = (self.min_avg * (self.min_avg_ix - 1.0) + min_temp) / self.min_avg_ix
        self.max_avg = (self.max_avg * (self.max_avg_ix - 1.0) + max_temp) / self.max_avg_ix
        if (gb.DIAG_LEVEL & gb.TPH_CUR_MO_AVG):
           gb.logging.info("POST: self.month: %d, month: %d" %
                           (self.month, month))
           gb.logging.info("POST: self.min_avg_ix %.1f self.min_avg %.1f" %
                           (self.min_avg_ix, self.min_avg))
           gb.logging.info("POST: self.max_avg_ix %.1f self.max_avg %.1f" %
                           (self.max_avg_ix, self.max_avg))

        self.avg_min_max_tempf_update(month_str, db_q_out)

    ########################################
    #
    # MonthyDataThread run function, including main loop
    # and received-message processing
    #
    ########################################
    def run(self):

        avg_q_in = self.args[0]
        db_q_out = self.args[1]
        end_event = self.args[2]

        gb.logging.info("Running %s" % (self.name))

        #---------------------------------------
        # Get day/night temperature running average info from DB
        #---------------------------------------
        cur_time = gb.datetime.now()

        db_msgType = db.DB_INIT_AVG_TEMP
        dbInfo = []
        dbInfo.append(db_msgType)
        dbInfo.append(cur_time.month)
        #if (gb.DIAG_LEVEL & 0x8):
        gb.logging.info("Sending %s(%d)" %
                            (db.get_db_msg_str(db_msgType),db_msgType))
        db_q_out.put(dbInfo)
        gb.time.sleep(1)
        
        #---------------------------------------
        #
        # AveragingThread init wait LOOP
        #
        #---------------------------------------
        init_in_progress = True

        #init1 = True
        #if (gb.DISABLE_RMT == True):
            # Disabled calls to remote weather report.  Previously
            # used to avoid exceeding calls to remote weather service
            # during testing, but now it is permanently disabled
        #    init1 = False

        init1 = True
        init2 = True
        resent = False
        temp_avg_data = []

        while not end_event.isSet() and init_in_progress == True:
            while not avg_q_in.empty() and init_in_progress == True:
                avg_data = avg_q_in.get()
                avg_msgType = avg_data[0]

                if (gb.DIAG_LEVEL & 0x800):
                    gb.logging.info("A: Recvd: %s(%d)" %
                        (avg.get_avg_msg_str(avg_msgType),
                         avg_msgType))

                if (avg_msgType == avg.AVG_SUNTIMES):
                    self.avg_daily_suntimes(avg_data)
                    gb.logging.info("init1 set to False")
                    init1 = False

                elif (avg_msgType == avg.AVG_INIT):
                    self.init_running_avg(avg_data)
                    gb.logging.info("init2 set to False")
                    init2 = False

                else:
                    if (avg_msgType == avg.AVG_DAY_HIGH_LOW and not resent):
                        gb.logging.info("Saving AVG_DAY_HIGH_LOW")
                        temp_avg_data = avg_data
                        resent = True
                    else:
                        gb.logging.error("*** Ignoring message type: %d" %
                                         (avg_msgType))
                        gb.logging.error(avg_data)

                gb.time.sleep(1) # sleep time for avg_q_in

            if (init1 == False and init2 == False):
                init_in_progress = False
            else:
                gb.time.sleep(1) # sleep time for while loop

        if (resent):
            if (temp_avg_data):
                avg_q_in.put(temp_avg_data)

        gb.logging.info("Monthly statistics initialized from DB")

        while not end_event.isSet():
            while not avg_q_in.empty():
                avg_data = avg_q_in.get()
                if (not avg_data):
                    gb.logging.info("avg_data is empty")
                    gb.logging.info("avg_data size: %d" % (len(avg_data)))    
                    print("avg_data: ",avg_data)
                avg_msgType = avg_data[0]

                if (gb.DIAG_LEVEL & 0x800):
                    gb.logging.info("B: Recvd: %s(%d)" %
                            (avg.get_avg_msg_str(avg_msgType),
                             avg_msgType))

                if (avg_msgType == avg.AVG_SUNTIMES):
                    # AVG_SUNTIMES sent from weather.py whenever weather.py
                    # obtains remote weather readings.  In-memory data
                    # updated only if current day does not match
                    # day in remote reading
                    self.avg_daily_suntimes(avg_data)

                elif (avg_msgType == avg.AVG_TEMPF):
                    # AVG_TEMPF sent from weather.py whenever weather.py
                    # also updates readings into DB (every 600 seconds)
                    self.monthly_running_avg_f(avg_data, db_q_out)

                elif (avg_msgType == avg.AVG_DAY_HIGH_LOW):
                    # AVG_DAY_HIGH_LOW sent from weather.py at end of
                    # day.  Contains day high/low, etc.
                    gb.logging.info("Recvd: %s(%d)" %
                                    (avg.get_avg_msg_str(avg_msgType),
                                     avg_msgType))
                    self.avg_min_max_tempf(avg_data, db_q_out)

                else:
                    gb.logging.error("Invalid monthly message type: %d" %
                                     (avg_msgType))
                    gb.logging.error(avg_data)

            gb.time.sleep(5)

        gb.logging.info("Exiting %s" % (self.name))
