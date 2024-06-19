import os

from datetime import datetime
from dateutil.relativedelta import relativedelta
import papermill as pm

from openhexa.sdk import current_run, pipeline, parameter, workspace


@pipeline(code="contribution-extracts", name="contribution_extracts")
@parameter(
    "date_year",
    name="Extract year (e.g: 2024)",
    help="Execute pipeline for specified year",
    type=str,
    default="2024",
    required=False,
)
@parameter(
    "date_month",
    name="Extract month (e.g: 02)",
    help="Execute pipeline for specified month",
    type=str,
    default="03",
    required=False,
)
def contribution_extracts(date_year:str, date_month:str):
    """
    In this pipeline we call a notebook launcher that executes the DSE tables update for COUSP local DB
    
    """

    # Setup variables
    notebook_name = "graphQL_contributions_Niger"  
    notebook_path = f"{workspace.files_path}/pipelines/Contribution_extracts/"
    out_notebook_path = f"{workspace.files_path}/pipelines/Contribution_extracts/papermill_outputs"
    
    # previous_month_date = current_date - relativedelta(months=1)
    if not valid_year_check(date_year):            
        return 
    
    if not valid_month_check(date_month):
        return 
    
    date_month = int(date_month)
    parameters = {
        'date_run': f"{date_year}-{date_month:02d}"
    }

    current_run.log_info(f"Executing for: {date_year}-{date_month:02d}")
    
    # Run notebook     
    run_update_with(nb_name=notebook_name, nb_path=notebook_path, out_nb_path=out_notebook_path, parameters=parameters) 



@contribution_extracts.task
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
    current_date = datetime.now()
    
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
    contribution_extracts()