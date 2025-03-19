import azure-functions as func
from .additional_functions import process_task

app = func.FunctionApp()

@app.function_name(name="TimerTrigger1")
@app.schedule(
    schedule="0 */5 * * * *",  # Cron expression: every 5 minutes
    arg_name="timer",  # Argument name passed to the function
    run_on_startup=False  # You can set this to True if you want it to run immediately when the function starts
)
def main(timer: func.TimerRequest) -> None:
    """
    Timer Trigger function that runs every 5 minutes. This example could be
    used to perform tasks such as cleaning up logs, checking statuses, etc.
    """
    # Check if the function was triggered (TimerRequest has a 'last' property)
    if timer.past_due:
        logging.info("The timer is past due!")

    # Call an additional function to perform some task
    process_task()

    # You can log the last run time or any other info you need
    logging.info(f"Timer function executed at {timer.utc_now}")
