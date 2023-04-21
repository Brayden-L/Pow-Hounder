from ph_general_funcs import perform_snow_scrape
import time

while True:
    try:
        perform_snow_scrape()
    except Exception as e:
        print(e)
        time.sleep(5)
        continue
