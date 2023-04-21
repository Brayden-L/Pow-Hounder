from ph_general_funcs import perform_lift_scrape
import time

while True:
    try:
        perform_lift_scrape()
    except Exception as e:
        print(e)
        time.sleep(5)
        continue
