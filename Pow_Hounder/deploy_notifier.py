from ph_general_funcs import lift_status_notifier
import time

while True:
    try:
        lift_status_notifier()
    except Exception as e:
        print(e)
        time.sleep(5)
        continue
