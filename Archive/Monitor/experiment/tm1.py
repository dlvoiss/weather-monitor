import time
from datetime import datetime
from datetime import timedelta

SECS_IN_A_DAY = 86400.0

# Get date/time for local timezone
def get_localdate_str():
    tm_str = str(datetime.now())
    return tm_str

def get_time_with_minutes(date_str):
    tm_str = re.sub('^....-..-.. ', "", date_str)
    tm_str = re.sub(':..\.......$', "", tm_str)
    return tm_str

def get_date_with_seconds(date_str):
    tm_str = re.sub('\.......$', "", date_str)
    return tm_str

print("Init earlier")
earlier = datetime.now()
time.sleep(20)
print("Init now")
now = datetime.now()
delta = now - earlier
sec = delta.total_seconds()
print("Seconds: ", sec)
days = sec / SECS_IN_A_DAY
print("Days: ", days)
