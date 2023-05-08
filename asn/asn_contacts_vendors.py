import os
import yaml
import pendulum
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import timedelta, datetime


env_type = os.environ['env_type']
error_email = os.environ['error_email']
emp_dag_path = '/home/airflow/gcs/dags/asn_contacts_vendors/dags/'
project_id = "backcountry-data-team" 
dataset_id = "elcap"
send_email = True


#Call the yaml file with the root
yamlFile = emp_dag_path + '_yaml/asn_contacts_vendors.yaml'
local_tz = pendulum.timezone('America/Denver')

  


#Call the information about the yaml file
with open(yamlFile, 'r') as key_file:
    configDict = yaml.safe_load(key_file.read())
    cloudstorageyaml = configDict['asn_contacts_vendors']
    bucket_name = cloudstorageyaml['bucket_name']
    project_id = cloudstorageyaml['project_id']
    today = "{{ ds_nodash }}"
    ns_transactions = cloudstorageyaml['ns_transactions']
    ns_transaction_lines = cloudstorageyaml['ns_transaction_lines']
    ns_sc = cloudstorageyaml['ns_sc']
    ns_entity = cloudstorageyaml['ns_entity']
    ns_2 = cloudstorageyaml['ns_2']
    ns_items = cloudstorageyaml['ns_items']
    ns_vendor_asn = cloudstorageyaml['ns_vendor_asn']
    asn_details_table = cloudstorageyaml['asn_details_table']
    asn_summary_table = cloudstorageyaml['asn_summary_table']
    contact_details_table = cloudstorageyaml['contact_details_table']
    carrier_shipment_table = cloudstorageyaml['carrier_shipment_table']
    edi_chr_po_vendor_fields_table = cloudstorageyaml['edi_chr_po_vendor_fields_table']
    vendor_contacts_table = cloudstorageyaml['vendor_contacts_table']
    sql_ASN_details = cloudstorageyaml['sql_ASN_details']
    sql_ASN_summary = cloudstorageyaml['sql_ASN_summary']
    sql_carrier_shipment = cloudstorageyaml['sql_carrier_shipment']
    sql_contact_details = cloudstorageyaml['sql_contact_details']
    sql_edi_chr_po_vendor_fields = cloudstorageyaml['sql_edi_chr_po_vendor_fields']
    sql_vendor_contacts = cloudstorageyaml['sql_vendor_contacts']

    #URI to file and create the file name

if env_type == 'dev':
    dataset_id = dataset_id + "_" + env_type
    send_email=False


    # passing the default arguments
default_args = {
    'owner': 'Fulfillment - Vendors ASN',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email': [error_email, 'biteam@backcountry.com'],
    'email_on_failure': send_email,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1)
    
}

with DAG('vendor_asn_chr'
        , default_args=default_args
        , max_active_runs=1
        , schedule_interval= "0 06 * * *"  # "@daily" 
        , catchup=False
        ) as dag:

    execute_sql_bq_vendor_fields = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id="execute_sql_bq_vendor_fields",
        sql="_sql/edi_chr_po_vendor_fields.sql",
        params={"ns_transactions": ns_transactions, "ns_transaction_lines": ns_transaction_lines, "ns_2": ns_2, "ns_vendor_asn": ns_vendor_asn,
                "edi_chr_po_vendor_fields_table": dataset_id +'.'+ edi_chr_po_vendor_fields_table}
    )
        
#Execute big query scritp to select and create the table
    execute_sql_bq_carrier_shipment = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id="execute_sql_bq_carrier_shipment",
        sql=sql_carrier_shipment,
        params={"ns_transactions": ns_transactions, "ns_transaction_lines": ns_transaction_lines, "ns_sc": ns_sc, "ns_entity":ns_entity,
                "ns_vendor_asn": ns_vendor_asn, "carrier_shipment_info_chr_table": dataset_id +'.'+ carrier_shipment_table}
    )
    
#Execute big query scritp to select and create the table
    execute_sql_bq_ASN_details = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id= "execute_sql_bq_ASN_details",
        sql= sql_ASN_details,
        params={"ns_transactions": ns_transactions, "ns_transaction_lines": ns_transaction_lines, "ns_sc": ns_sc, "ns_entity":ns_entity,
                "ns_2": ns_2, "ns_items": ns_items, "ns_vendor_asn": ns_vendor_asn, "asn_details_table": dataset_id +'.'+ asn_details_table  }
    )
    
#Execute big query scritp to select and create the table
    execute_sql_bq_ASN_summary = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id="execute_sql_bq_ASN_summary",
        sql=sql_ASN_summary,
        params={"ns_transactions": ns_transactions, "ns_transaction_lines": ns_transaction_lines, "ns_sc": ns_sc, "ns_entity":ns_entity,
                "ns_2": ns_2, "ns_items": ns_items, "ns_vendor_asn": ns_vendor_asn, "asn_summary_table": dataset_id +'.'+ asn_summary_table  }
    )
    
#Execute big query scritp to select and create the table
    execute_sql_bq_contact_details = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id="execute_sql_bq_contact_details",
        sql=sql_contact_details,
        params={ "ns_sc": ns_sc, "ns_entity": ns_entity, "ns_2": ns_2,
                "ns_vendor_asn": ns_vendor_asn, "contact_details_table": dataset_id + '.' + contact_details_table}
    )
    
#Execute big query scritp to select and create the table
    execute_sql_bq_vendor_contacts = BigQueryOperator(
        dag=dag,
        use_legacy_sql=False,
        priority='BATCH',
        task_id="execute_sql_bq_vendor_contacts",
        sql=sql_vendor_contacts,
        params={"ns_sc": ns_sc, "ns_2": ns_2,
                "ns_vendor_asn": ns_vendor_asn, "vendor_contacts_table": dataset_id + '.' + vendor_contacts_table}
    )

[execute_sql_bq_vendor_fields , execute_sql_bq_carrier_shipment , execute_sql_bq_ASN_details, execute_sql_bq_ASN_summary ,
 execute_sql_bq_contact_details , execute_sql_bq_vendor_contacts]
