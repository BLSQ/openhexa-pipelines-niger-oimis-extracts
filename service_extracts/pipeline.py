import os

from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
import papermill as pm
import calendar

from openhexa.sdk import current_run, pipeline, parameter, workspace


@pipeline(code="service-extracts", name="service_extracts")
@parameter(
    "date_year_from",
    name="Extract year from (e.g: 2024)",
    help="Execute pipeline from specified year",
    type=str,
    default=None,
    required=False,
)
@parameter(
    "date_month_from",
    name="Extract month from (e.g: 01)",
    help="Execute pipeline from specified month",
    type=str,
    default=None,
    required=False,
)
@parameter(
    "date_year_to",
    name="Extract year to (e.g: 2024)",
    help="Execute pipeline until specified year",
    type=str,
    default=None,
    required=False,
)
@parameter(
    "date_month_to",
    name="Extract month to (e.g: 03)",
    help="Execute pipeline untlil specified month",
    type=str,
    default=None,
    required=False,
)
@parameter(
    "run_auto",
    name="Automatic run (previous month)",
    help="Execute pipeline for previous month",
    type=bool,
    default=True,
    required=False,
)
def service_extracts(run_auto:bool, date_year_from:str, date_month_from:str, date_year_to:str, date_month_to:str):
    """
    In this pipeline we call a notebook that execute the service openIMIS extracts
    
    """

    # Setup variables
    notebook_name = "graphQL_services_Niger"  
    notebook_path = f"{workspace.files_path}/pipelines/Service_extracts" 
    out_notebook_path = f"{workspace.files_path}/pipelines/Service_extracts/papermill_outputs"
    
    if run_auto:
        dateFrom_Gte, dateFrom_Lte = get_previous_month_dates()
        current_run.log_info(f"Automatic run from: {dateFrom_Gte} to: {dateFrom_Lte}")    

    else:
        # Date input checks - FROM
        if date_year_from is None and date_month_from is None:
            dateFrom_Gte = None # then we dont filter .. download everything
        else:
            if not valid_year_check(date_year_from):            
                return 
            if not valid_month_check(date_month_from):
                return             
            date_month_from = int(date_month_from)
            dateFrom_Gte = f"{date_year_from}-{date_month_from:02d}-01"
            current_run.log_info(f"Retrieve data from: {dateFrom_Gte}")
        
        # Date input checks - TO
        if date_year_to is None or date_month_to is None:
            dateFrom_Lte = None
        else:
            if not valid_year_check(date_year_to):            
                return 
            if not valid_month_check(date_month_to):
                return
            date_month_to = int(date_month_to)
            dateFrom_Lte = get_last_day_of_month(f"{date_year_to}-{date_month_to:02d}")        
            current_run.log_info(f"Retrieve data to: {dateFrom_Lte}")
            
    parameters = {
        'dateFrom_Gte': dateFrom_Gte,
        'dateFrom_Lte': dateFrom_Lte
    }

    current_run.log_info(f"Executing pipeline.")
    
    # Run notebook     
    run_update_with(nb_name=notebook_name, nb_path=notebook_path, out_nb_path=out_notebook_path, parameters=parameters)



@service_extracts.task
def run_update_with(nb_name:str, nb_path:str, out_nb_path:str, parameters:dict):
    """
    Update a tables using the latest dataset version
    
    """         
    nb_full_path = os.path.join(nb_path, f"{nb_name}.ipynb")
        
    current_run.log_info(f"Executing notebook: {nb_full_path}")

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    out_nb_fname = f"{nb_name}_OUTPUT_{execution_timestamp}.ipynb" 
    out_nb_full_path = os.path.join(out_nb_path, out_nb_fname)

    pm.execute_notebook(input_path = nb_full_path,
                        output_path = out_nb_full_path,
                        parameters=parameters)



def get_previous_month_dates():
    # Get the current date
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    # Format the results
    firstMonthDay = first_day_of_previous_month.strftime('%Y-%m-%d')
    lastlMonthDay = last_day_of_previous_month.strftime('%Y-%m-%d')

    return firstMonthDay, lastlMonthDay


def get_last_day_of_month(date_str):
    # Parse the input date string
    date = datetime.strptime(date_str, "%Y-%m")
    last_day = calendar.monthrange(date.year, date.month)[1]
    last_day_date = date.replace(day=last_day)
    
    # Format the last day
    return last_day_date.strftime("%Y-%m-%d")


# check the year inputs
def valid_year_check(d_year:str):
    current_date = datetime.now()
    
    try:
        d_year = int(d_year)
    except Exception as e:
        current_run.log_error(f"Invalid year input. Please follow year format YYYY. ERROR:{e}")
        return False

    if d_year < 2000 or d_year > current_date.year:
        # raise Exception(f"Please enter values between 2000 and {current_date.year}")
        current_run.log_error(f"Please enter year values between 2000 and {current_date.year}")
        return False
    
    return True
        

# check the month inputs
def valid_month_check(d_month:str):
    # current_date = datetime.now()
    
    try:
        d_month = int(d_month)
    except Exception as e:
        current_run.log_error(f"Invalid month input. Please follow month format MM. ERROR:{e}")
        return False

    if d_month < 1 or d_month > 12:
        current_run.log_error(f"Please enter month values between 1 and 12")
        return False
    
    return True
 

if __name__ == "__main__":
    service_extracts()