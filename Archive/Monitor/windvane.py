import gb

import wv
import co

import signal
import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn

EAST_START       =     1
EAST_END         =  4400 #  < 4400
SOUTH_EAST_START =  4400 # >= 4400
SOUTH_EAST_END   =  8999
SOUTH_START      =  9000
SOUTH_END        = 14850
NORTH_EAST_START = 14851
NORTH_EAST_END   = 22400 #  < 22400
SOUTH_WEST_START = 22400 # >= 24000
SOUTH_WEST_END   = 27000 #  < 27000
NORTH_START      = 27000 # >= 27000
NORTH_END        = 32650 #  < 32650
WEST_START       = 32650 # >= 32650
WEST_END         = 32768  # 2^15 = 32768
NORTH_WEST_START = 32650 # >= 32650
NORTH_WEST_END   = 32768  # 2^15 = 32768

###########################################################
# Wind Vane thread
###########################################################
class WindvaneThread(gb.threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                args=(), kwargs=None, verbose=None):
        gb.threading.Thread.__init__(self, group=group, target=target,
                                     name=name)
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.kill_received = False

        return

    def send_direction(self, nm, co_q_out, msg_in, wind_dir):
        req_id = msg_in[2]
        e_tm = msg_in[1]
        wind_dir_int = wv.wind_dir_str_to_int(wind_dir)
        msg = []
        msgType = co.CO_WIND_DIR
        msg.append(msgType)
        msg.append(e_tm)
        msg.append(req_id)
        msg.append(wind_dir_int)
        if (gb.DIAG_LEVEL & gb.WIND_DIR_MSG):
            gb.logging.info("%s sending %s(%d)" %
                            (nm, co.get_co_msg_str(msgType), msgType))
        co_q_out.put(msg)

    ###########################################################
    # Wind Vane thread run loop
    ###########################################################
    def run(self):
        gb.logging.info("Running %s thread" % (self.name))
        if (gb.DIAG_LEVEL & gb.WIND_DIR_DETAIL):
            gb.logging.info(self.args)

        wv_q_in = self.args[0]
        co_q_out = self.args[1]
        end_event = self.args[2]

        # ADS115 is 16-bit ADC
        i2c = busio.I2C(board.SCL, board.SDA)
        # Create the ADS object and specify the gain
        ads = ADS.ADS1115(i2c)
        # Can change based on the voltage signal - Gain of 1 is typically
        # enough for a lot of sensors
        ads.gain = 1
        chan0 = AnalogIn(ads, ADS.P0)  # Windvane direction
        chan1 = AnalogIn(ads, ADS.P1)  # Unused
        chan2 = AnalogIn(ads, ADS.P2)  # Unused
        chan3 = AnalogIn(ads, ADS.P3)  # Unused
        gb.logging.info("ADS1115 (i2c) initialized")

        wv_dir = ""
        wv_dir2 = ""
        logging_time_next = gb.datetime.now()

        while not end_event.isSet():
            wv_data = ""
            while (not wv_q_in.empty()):
                msg = wv_q_in.get()
                msgType = msg[0]
                if ((gb.DIAG_LEVEL & gb.WIND_DIR_MSG) and
                    (gb.DIAG_LEVEL & gb.WIND_DIR_DETAIL)):
                    gb.logging.info("%s: Received %s(%d)" %
                            (self.name, wv.get_wv_msg_str(msgType), msgType))

                if (msgType == wv.WV_EXIT):
                    gb.logging.info("%s: Cleanup prior to exit" % (self.name))

                elif (msgType == wv.WV_GET_DIRECTION):
                    if (gb.DIAG_LEVEL & gb.WIND_DIR_MSG):
                        gb.logging.info("%s: Received %s(%d)" %
                            (self.name, wv.get_wv_msg_str(msgType), msgType))
                    self.send_direction(self.name, co_q_out, msg, wv_dir)

                else:
                    gb.logging.error("Invalid WD message type: %d" % (msgType))
                    gb.logging.error(msg)

                gb.time.sleep(0.1)

            wv_volts = chan0.voltage
            wv_value = chan0.value
            wv_dir = ""
            wv_dir2 = ""

            if ((wv_value >= EAST_START) and (wv_value < EAST_END)):
                wv_dir = wv.EAST
            elif ((wv_value >= SOUTH_EAST_START) and (wv_value <= SOUTH_EAST_END)):
                wv_dir = wv.SOUTH_EAST
            elif ((wv_value >= SOUTH_START) and (wv_value <= SOUTH_END)):
                wv_dir = wv.SOUTH
            elif ((wv_value >= NORTH_EAST_START) and (wv_value <= NORTH_EAST_END)):
                wv_dir = wv.NORTH_EAST
            elif ((wv_value >= SOUTH_WEST_START) and (wv_value <= SOUTH_WEST_END)):
                wv_dir = wv.SOUTH_WEST
            elif ((wv_value >= NORTH_START) and (wv_value <= NORTH_END)):
                wv_dir = wv.NORTH
            elif ((wv_value >= WEST_START) and (wv_value <= WEST_END)):
                wv_dir = wv.WEST
                wv_dir2= wv.NORTH_WEST
            else:
                wv_dir = "Invalid"
                logging_now = gb.datetime.now()
                if (logging_now > logging_time_next):
                    logging_time_next =  logging_now + gb.timedelta(seconds=60)
                    gb.logging.error("ERROR: Wind direction: %s, %d, %0.5f" %
                                     (wv_dir, wv_value, wv_volts))

            #gb.logging.info("Wind Dir: %s %s %5d %.5f" %
            #                 (wv_dir, wv_dir2, wv_value, wv_volts))
            #gb.time.sleep(5)  # one-second in non-debug

        if (gb.DIAG_LEVEL & gb.WIND_DIR_DETAIL):
            gb.logging.info("Wind Dir: %s %s %5d %.5f" %
                         (wv_dir, wv_dir2, wv_value, wv_volts))
        elif (gb.DIAG_LEVEL & gb.WIND_DIR):
            gb.logging.info("Wind Dir: %s %s" % (wv_dir, wv_dir2))

            if (gb.DIAG_LEVEL & gb.WIND_DIR_DETAIL):
                gb.time.sleep(5)
            else:
                gb.time.sleep(1)

        gb.logging.info("Exiting thread %s" % (self.name))
        return
