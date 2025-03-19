import logging
from datetime import datetime

def main(myTimer: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Writing the timestamp to a file each time the function runs
    with open("/tmp/timer_log.txt", "a") as file:
        file.write(f"Triggered at: {timestamp}\n")
    
    logging.info(f"Timer triggered at: {timestamp}")
