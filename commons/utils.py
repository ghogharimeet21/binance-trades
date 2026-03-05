import numpy as np
from datetime import datetime, timedelta
import pytz




def return_1Darr_to_str(arr: np.ndarray):
    return f"date={int(arr[0])} | time={seconds_to_hms(int(arr[1]))} | open={arr[2]} high={arr[3]} low={arr[4]} close={arr[5]} volume={arr[6]} oi={arr[7]} | log_time={seconds_to_hms(int(arr[8]))}"


def get_prev_date(date: int, days: int) -> int:
    dt = datetime.strptime(str(date), "%y%m%d")
    if prev_dt := dt - timedelta(days=days):
        return int(prev_dt.strftime("%y%m%d"))
    prev_dt = dt - timedelta(days=1)
    return int(prev_dt.strftime("%y%m%d"))


def seconds_to_hms(seconds: int) -> str:
    """Convert seconds to HH:MM:SS format with validation"""
    if seconds < 0:
        raise ValueError(f"seconds={seconds} cannot be negative")
    if seconds >= 86400:  # 24 * 3600
        raise ValueError(f"seconds={seconds} exceeds 24 hours (86400 seconds)")
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def hms_to_seconds(time_str: str) -> int:
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours > 24:
        raise ValueError(
            f"in {time_str} hour={hours} is not valid please enter less then 24"
        )
    if minutes > 59:
        raise ValueError(
            f"in {time_str} minute={minutes} is not valid please enter less then 60"
        )
    if seconds > 59:
        raise ValueError(
            f"in {time_str} seconds={seconds} is not valid please enter less then 60"
        )
    return (hours * 3600) + minutes * 60 + seconds




def filter_date_time(ts_ms, tz="Asia/Kolkata"):
    tz_obj = pytz.timezone(tz)
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=tz_obj)
    
    date_int = int(dt.strftime("%y%m%d"))
    time_int = hms_to_seconds(dt.strftime("%H:%M:%S"))
        
    return date_int, time_int



def extract_digits(text: str):
    try:
        return int(''.join(filter(str.isdigit, text)))
    except ValueError:
        raise ValueError("No digits found")