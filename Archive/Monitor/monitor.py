import gb

import signal

import db
import fan
import wthr
import co
import wv
import an
import rg

import database
import fanthread
import weather
import average
import coordinator
import windvane
import anemometer
import raingauge

thread_group = []

CONFIG_DELAY = 1    # 1 second

#####################
# Destroy
#####################
def destroy():
    gb.logging.info("Cleaning up prior to exit")
    gb.time.sleep(CONFIG_DELAY)
    gb.GPIO.output(gb.FAN_PIN, gb.GPIO.LOW)  # FAN off
    gb.GPIO.cleanup()

#####################
# Setup
#####################
def setup():
    tm_str = gb.get_date_with_seconds(gb.get_localdate_str())
    gb.logging.info("DIAG_LEVEL 0x%x" % (gb.DIAG_LEVEL))
    gb.logging.info("Running setup() at: %s" % (tm_str))
    gb.logging.info("Setting up Raspberry PI pinouts")
    gb.GPIO.setmode(gb.GPIO.BCM)
    #####################
    # Setup pin outs for solar valve relay
    #####################
    gb.logging.info("Setting up GPIO for CPU fan control")
    gb.GPIO.setup(gb.FAN_PIN, gb.GPIO.OUT)
    gb.GPIO.output(gb.FAN_PIN, gb.GPIO.LOW)  # FAN off
    gb.logging.info("Setting up GPIO for Rain Gauge")
    gb.GPIO.setup(gb.RAIN_GAUGE_GPIO, gb.GPIO.IN)
    gb.logging.info("Setting up GPIO for Anemometer")
    gb.GPIO.setup(gb.ANEMOMETER_GPIO, gb.GPIO.IN, pull_up_down=gb.GPIO.PUD_UP)
    gb.time.sleep(CONFIG_DELAY)

def notify_db(db_q):
    msg = []
    msgType = db.DB_EXIT
    msg.append(msgType)
    gb.logging.info("Sending %s(%d) to database thread" %
                    (db.get_db_msg_str(msgType), msgType))
    db_q.put(msg)

def notify_co(co_q):
    msg = []
    msgType = co.CO_EXIT
    msg.append(msgType)
    gb.logging.info("Sending %s(%d) to coordinator thread" %
                    (co.get_co_msg_str(msgType), msgType))
    co_q.put(msg)

def notify_wv(wv_q):
    msg = []
    msgType = wv.WV_EXIT
    msg.append(msgType)
    gb.logging.info("Sending %s(%d) to windvane thread" %
                    (wv.get_wv_msg_str(msgType), msgType))
    wv_q.put(msg)

def notify_an(an_q):
    msg = []
    msgType = an.AN_EXIT
    msg.append(msgType)
    gb.logging.info("Sending %s(%d) to anemometer process" %
                    (an.get_an_msg_str(msgType), msgType))
    an_q.put(msg)

def notify_rg(rg_q):
    msg = []
    msgType = rg.RG_EXIT
    msg.append(msgType)
    gb.logging.info("Sending %s(%d) to rain gauge process" %
                    (rg.get_rg_msg_str(msgType), msgType))
    rg_q.put(msg)

def has_live_threads(thrdGrp):
    return True in [t.isAlive() for t in thrdGrp]

def receive_TERM(signalNumber, frame):

    global end_script

    gb.logging.info("Received SIGTERM(%d)" % (signalNumber))
    #end_event.set()
    end_script = True

##############################################
# main function
##############################################
if __name__ == '__main__':

    global end_script

    end_script = False

    my_pid = gb.os.getpid()
    gb.logging.info("PID: %d" % (my_pid))
    signal.signal(signal.SIGTERM,receive_TERM)

    setup()

    cores = gb.MP.cpu_count()
    gb.logging.info("Core count = %d" % cores)

    ##################################################
    # Setup I/O queues used to communicate between
    # processes and threads
    ##################################################
    rg_io_queue = gb.MP.Queue()
    an_io_queue = gb.MP.Queue()
    co_mp_io_queue = gb.MP.Queue()
    db_io_queue = gb.Queue() # Create queue for DB thread
    cpufan_io_queue = gb.Queue() # Create queue for CPU Fan thread
    weather_io_queue = gb.Queue() # Create queue for weather thread
    avg_io_queue = gb.Queue() # Create queue queue for monthly stats
    co_io_queue = gb.Queue()
    wind_vane_io_queue = gb.Queue()

    end_event = gb.threading.Event()

    #------------------------------------------------
    # Create database thread
    #------------------------------------------------
    databaseThrd = database.DatabaseThread(name="DBThread",
                          args=(db_io_queue, cpufan_io_queue, weather_io_queue,
                                avg_io_queue, end_event))
    thread_group.append(databaseThrd)

    #------------------------------------------------
    # Create CPU Fan thread
    #------------------------------------------------
    cpufanThrd = fanthread.CPUFanControlThread(name="CPUFanControlThread",
                          args=(cpufan_io_queue,db_io_queue,end_event))
    thread_group.append(cpufanThrd)

    #------------------------------------------------
    # Create Weather Monitor thread
    #------------------------------------------------
    weatherThrd = weather.WeatherThread(name="WeatherThread",
                          args=(weather_io_queue, db_io_queue,
                                avg_io_queue, end_event))
    thread_group.append(weatherThrd)

    #-----------------------------------------------------
    # Create wind and rain coordinator thread
    #-----------------------------------------------------
    coordinatorThrd = coordinator.CoordinatorThread(name="CoordinatorThread",
                          args=(co_io_queue,co_mp_io_queue, wind_vane_io_queue,
                                rg_io_queue, db_io_queue, end_event))
    thread_group.append(coordinatorThrd)

    #-----------------------------------------------------
    # Create windvane (wind direction) thread
    #-----------------------------------------------------
    windvaneThrd = windvane.WindvaneThread(name="WindVaneThread",
                          args=(wind_vane_io_queue,co_io_queue, end_event))
    thread_group.append(windvaneThrd)

    #------------------------------------------------
    # Create Average Monthly Statistics thread
    #------------------------------------------------
    averagingThrd = average.AveragingThread(name="AveragingThread",
                          args=(avg_io_queue, db_io_queue, end_event))
    thread_group.append(averagingThrd)

    ##############################################
    # Create Processes
    ##############################################
    #-----------------------------------------------------
    # Rain Gauge is CPU intensive, so run on separate processor
    #-----------------------------------------------------
    p1_name = "Rain Gauge"
    p1 = gb.MP.Process(name=p1_name, target = raingauge.rain_gauge, args=((rg_io_queue),(co_mp_io_queue),))
    p1.start()
    gb.logging.info("Started %s" % (p1_name))

    #-----------------------------------------------------
    # Anemometer is CPU intensive, so run on separate processor
    #-----------------------------------------------------
    p2_name = "Anemometer"
    p2 = gb.MP.Process(name=p2_name, target = anemometer.anemometer, args=((an_io_queue),(co_mp_io_queue),))
    p2.start()
    gb.logging.info("Started %s" % (p2_name))

    ##################################################
    # Start the threads
    ##################################################
    databaseThrd.start()
    cpufanThrd.start()
    weatherThrd.start()
    averagingThrd.start()
    coordinatorThrd.start()
    windvaneThrd.start()

    tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

    ##################################################
    # Monitor threads for shutdown; shutdown triggered via:
    # - kill -15 <pid> used to terminate script.  The Pi is not
    #   halted via this option.
    # - Ctrl-c used to terminate script.  The Pi is not halted
    #   via this option.
    ##################################################

    end_notification_sent = False

    while has_live_threads(thread_group):
        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

        if (end_script == True):
            if (end_notification_sent == False):
                notify_db(db_io_queue)
                notify_co(co_io_queue)
                notify_wv(wind_vane_io_queue)
                notify_an(an_io_queue)
                notify_rg(rg_io_queue)
                end_notification_sent = True
            gb.time.sleep(2)
            end_event.set()

        try:
            # synchronization timeout of threads kill
            [t.join(1) for t in thread_group if t is not None and t.isAlive()]

            gb.time.sleep(5)

        except KeyboardInterrupt:
            gb.logging.info("%s: Stopping script" % (tm_str))
            end_script = True

        gb.time.sleep(2)

    destroy()
    gb.logging.info("%s: MAIN EXIT" % (tm_str))
