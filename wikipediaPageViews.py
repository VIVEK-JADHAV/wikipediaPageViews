import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

dag = DAG(
  dag_id="wikipediaPageViews",
  start_date=airflow.utils.dates.days_ago(0),
  schedule_interval="@hourly",
  catchup=False,
  template_searchpath="/tmp"
)

get_data = BashOperator(
  task_id="get_data",
  bash_command=(
    "curl -o /tmp/wikipageviews.gz "
    "https://dumps.wikimedia.org/other/pageviews/"
    "{{ execution_date.year }}/"                          
    "{{ execution_date.year }}-"
    "{{ '{:02}'.format(execution_date.month) }}/"
    "pageviews-{{ execution_date.year }}"
    "{{ '{:02}'.format(execution_date.month) }}"
    "{{ '{:02}'.format(execution_date.day) }}-"
    "020000.gz"
    # "{{ '{:02}'.format(execution_date.hour) }}0000.gz"    
  ),
  dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag,
)

def _fetch_pageviews(pagenames,**context):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews", "r") as f:                          
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")    
            if domain_code == "en" and page_title in pagenames:          
                result[page_title] = view_counts
 
    with open("/tmp/sqlserver_query.sql", "w") as f:
       for pagename, pageviewcount in result.items():             
           f.write(
               "INSERT INTO pageview_counts VALUES ("
               f"'{pagename}', {pageviewcount}, '{context['execution_date']}'"
               ");\n"
           )

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        }
    },
    dag=dag,
)

write_to_sqlserever = MsSqlOperator(
   task_id="write_to_sqlserever",
   mssql_conn_id="my_sqlserver",            
   sql="sqlserver_query.sql", 
   database="master",                 
   dag=dag,
)

get_data>>extract_gz>>fetch_pageviews>>write_to_sqlserever
