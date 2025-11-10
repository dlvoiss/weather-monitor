import gb

import signal

import db
import fan
import wthr

import database
import fanthread
import weather

thread_group = []

CONFIG_DELAY = 1    # 1 second

def destroy():
    gb.logging.info("Cleaning up prior to exit")
    gb.time.sleep(CONFIG_DELAY)
    gb.GPIO.output(gb.FAN_PIN, gb.GPIO.LOW)  # FAN off
    gb.GPIO.cleanup()

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
    gb.time.sleep(CONFIG_DELAY)

def has_live_threads(thrdGrp):
    return True in [t.isAlive() for t in thrdGrp]

def receive_TERM(signalNumber, frame):
    gb.logging.info("Received SIGTERM(%d)" % (signalNumber))
    end_event.set()

if __name__ == '__main__':
    my_pid = gb.os.getpid()
    gb.logging.info("PID: %d" % (my_pid))
    signal.signal(signal.SIGTERM,receive_TERM)
    setup()

    ##################################################
    # Setup I/O queues used to send messages to display panel
    # and to database thread
    ##################################################
    db_io_queue = gb.Queue() # Create queue for DB thread
    cpufan_io_queue = gb.Queue() # Create queue for CPU Fan thread
    weather_io_queue = gb.Queue() # Create queue for weather thread

    end_event = gb.threading.Event()

    ##################################################
    # Create database thread
    ##################################################
    databaseThrd = database.DatabaseThread(name="DBThread",
                          args=(db_io_queue, cpufan_io_queue,
                                weather_io_queue, end_event))
    thread_group.append(databaseThrd)

    ##################################################
    # Create CPU Fan thread
    ##################################################
    cpufanThrd = fanthread.CPUFanControlThread(name="CPUFanControlThread",
                          args=(cpufan_io_queue,db_io_queue,end_event))
    thread_group.append(cpufanThrd)

    ##################################################
    # Create Weather Monitor thread
    ##################################################
    weatherThrd = weather.WeatherThread(name="WeatherThread",
                          args=(weather_io_queue, db_io_queue, end_event))
    thread_group.append(weatherThrd)

    ##################################################
    # Start the threads
    ##################################################
    databaseThrd.start()
    cpufanThrd.start()
    weatherThrd.start()

    tm_str = gb.get_date_with_seconds(gb.get_localdate_str())

    ##################################################
    # Monitor threads for shutdown; shutdown triggered via:
    # - kill -15 <pid> used to terminate script.  The Pi is not
    #   halted via this option.
    # - Ctrl-c used to terminate script.  The Pi is not halted
    #   via this option.
    ##################################################
    while has_live_threads(thread_group):
        tm_str = gb.get_date_with_seconds(gb.get_localdate_str())
        try:
            # synchronization timeout of threads kill
            [t.join(1) for t in thread_group if t is not None and t.isAlive()]

            gb.time.sleep(5)

        except KeyboardInterrupt:
            gb.logging.info("%s: Stopping script" % (tm_str))
            end_event.set()

    destroy()
    gb.logging.info("%s: MAIN EXIT" % (tm_str))
