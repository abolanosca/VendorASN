import os
import yaml
import pendulum
import datetime
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from datetime import timedelta
from airflow.models import Variable

env_type = os.environ['env_type']
error_email = os.environ['error_email']
emp_dag_path = Variable.get("vendor_compliance_asn")
send_email = True
# Call the yaml file with the root
yamlFile = emp_dag_path + '_yaml/vendor_compliance.yaml'
local_tz = pendulum.timezone('America/Denver')
now = datetime.datetime.now()

dag_id = 'vendors_asn_po'
date = pendulum.now(tz='America/Denver').strftime("%A %B %d %X")
date2 = pendulum.now(tz='America/Denver').strftime("%B%Y")
year = now.strftime("%Y")

# Call the information about the yaml file
with open(yamlFile, 'r') as key_file:
    configDict = yaml.safe_load(key_file.read())
    cloudstorageyaml = configDict['asn_config']  # RENAME---------
    bucket_name = cloudstorageyaml['bucket_name']
    project_id = cloudstorageyaml['project_id']
    dataset_id = cloudstorageyaml['dataset_id']
    purchase_order_all = cloudstorageyaml['purchase_order_all']
    transactions = cloudstorageyaml['transactions']
    employees = cloudstorageyaml['employees']
    transaction_lines = cloudstorageyaml['transaction_lines']
    entity = cloudstorageyaml['entity']
    po_types = cloudstorageyaml['po_types']
    season = cloudstorageyaml['season']
    year = cloudstorageyaml['year']
    payment_terms = cloudstorageyaml['payment_terms']
    dim_dates = cloudstorageyaml['dim_dates']
    allocation_channel = cloudstorageyaml['allocation_channel']
    merchandise_division = cloudstorageyaml['merchandise_division']
    locations = cloudstorageyaml['locations']
    integrationstatuses = cloudstorageyaml['integrationstatuses']
    po_category = cloudstorageyaml['po_category']
    ready_for_860_transmit = cloudstorageyaml['ready_for_860_transmit']
    purchase_order_change_status = cloudstorageyaml['purchase_order_change_status']
    departments = cloudstorageyaml['departments']
    acknowledgment_types = cloudstorageyaml['acknowledgment_types']
    stg_po_lines_nested = cloudstorageyaml['stg_po_lines_nested']
    items_cv = cloudstorageyaml['items_cv']
    sb_inventory = cloudstorageyaml['sb_inventory']
    uom = cloudstorageyaml['uom']
    brand = cloudstorageyaml['brand']
    acknowledgment_item_statuses = cloudstorageyaml['acknowledgment_item_statuses']
    product_group = cloudstorageyaml['product_group']
    sb_receipts = cloudstorageyaml['sb_receipts']
    carrier_shipment_chr_nested = cloudstorageyaml['carrier_shipment_chr_nested']
    carrier_shipping_data = cloudstorageyaml['carrier_shipping_data']
    carrier_shipments = cloudstorageyaml['carrier_shipments']
    stg_vendor_contacts = cloudstorageyaml['stg_vendor_contacts']
    vendors = cloudstorageyaml['vendors']
    vendor_types = cloudstorageyaml['vendor_types']
    address_book = cloudstorageyaml['address_book']
    currencies = cloudstorageyaml['currencies']
    edi_document_type_status = cloudstorageyaml['edi_document_type_status']
    vendor_subsidiary_map = cloudstorageyaml['vendor_subsidiary_map']
    contacts = cloudstorageyaml['contacts']
    contact_types = cloudstorageyaml['contact_types']
    message = cloudstorageyaml['message']
    finance_status = cloudstorageyaml['finance_status']
    subsidiaries = cloudstorageyaml['subsidiaries']
    vendor_subsidiary_map = cloudstorageyaml['vendor_subsidiary_map']
    operations_status = cloudstorageyaml['operations_status']
    stg_contact_details = cloudstorageyaml['stg_contact_details']
    companies = cloudstorageyaml['companies']
    vendor_edi_856_asn = cloudstorageyaml['vendor_edi_856_asn']
    edi_856_asn = cloudstorageyaml['edi_856_asn']
    vendor_edi_856_asn_line = cloudstorageyaml['vendor_edi_856_asn_line']
    edi_856_asn_line = cloudstorageyaml['edi_856_asn_line']
    purchase_order_nested = cloudstorageyaml['purchase_order_nested']
    vendor_contacts_nested = cloudstorageyaml['vendor_contacts_nested']

    today = "{{ ds_nodash }}"
    # URI to file and create the file name

if env_type == 'dev':
    send_email = False
    dataset_id = dataset_id + "_" + env_type

    # passing the default arguments
default_args = {
    'owner': 'Fulfillment - Vendors ASN',
    'start_date': datetime.datetime(2021, 6, 14, tzinfo=local_tz),
    'email': [error_email, 'biteam@backcountry.com'],
    'email_on_failure': send_email,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1)

}

with DAG(dag_id
        ,default_args=default_args
        ,max_active_runs=1
        ,schedule_interval="0 0 * * *"
        ,catchup=False
         ) as dag:
    # Execute big query scritp to create stg_purchase_orders_all
    create_stg_purchase_orders = bigquery_operator.BigQueryOperator(
        task_id="create_stg_purchase_orders",
        use_legacy_sql=False,
        sql="_sql/stg_purchase_orders_all.sql",
        params={
            "project_id": project_id,
            "purchase_order_all": purchase_order_all,
            "transactions": transactions,
            "transaction_lines": transaction_lines,
            "employees": employees,
            "entity": entity,
            "po_types": po_types,
            "season": season,
            "year": year,
            "payment_terms": payment_terms,
            "dim_dates": dim_dates,
            "allocation_channel": allocation_channel,
            "merchandise_division": merchandise_division,
            "locations": locations,
            "integrationstatuses": integrationstatuses,
            "po_category": po_category,
            "ready_for_860_transmit": ready_for_860_transmit,
            "purchase_order_change_status": purchase_order_change_status,
            "departments": departments,
            "acknowledgment_types": acknowledgment_types}
    )

    # Execute big query scritp to create stg_po_lines_nested
    create_stg_po_lines = bigquery_operator.BigQueryOperator(
        task_id="create_stg_po_lines",
        use_legacy_sql=False,
        sql="_sql/stg_po_lines_nested.sql",
        params={
            "project_id": project_id,
            "stg_po_lines_nested": stg_po_lines_nested,
            "transaction_lines": transaction_lines,
            "transactions": transactions,
            "items_cv": items_cv,
            "sb_inventory": sb_inventory,
            "uom": uom,
            "brand": brand,
            "acknowledgment_item_statuses": acknowledgment_item_statuses,
            "product_group": product_group,
            "sb_receipts": sb_receipts}
    )

    # Execute big query scritp to create stg_carrier_shimpment_chr
    create_stg_carrier = bigquery_operator.BigQueryOperator(
        task_id="create_stg_carrier",
        use_legacy_sql=False,
        sql="_sql/stg_carrier_shipment_chr.sql",
        params={
            "project_id": project_id,
            "carrier_shipment_chr_nested": carrier_shipment_chr_nested,
            "transactions": transactions,
            "locations": locations,
            "transaction_lines": transaction_lines,
            "carrier_shipping_data": carrier_shipping_data,
            "carrier_shipments": carrier_shipments,
            "entity": entity}
    )

    # Execute big query scritp to create stg_vendor_contacts
    create_stg_vendors = bigquery_operator.BigQueryOperator(
        task_id="create_stg_vendors",
        use_legacy_sql=False,
        sql="_sql/stg_vendor_contacts.sql",
        params={
            "project_id": project_id,
            "stg_vendor_contacts": stg_vendor_contacts,
            "vendors": vendors,
            "vendor_types": vendor_types,
            "payment_terms": payment_terms,
            "employees": employees,
            "address_book": address_book,
            "currencies": currencies,
            "edi_document_type_status": edi_document_type_status,
            "vendor_subsidiary_map": vendor_subsidiary_map,
            "contacts": contacts,
            "contact_types": contact_types,
            "message": message,
            "entity": entity,
            "operations_status": operations_status,
            "finance_status": finance_status,
            "subsidiaries": subsidiaries,
            "vendor_subsidiary_map": vendor_subsidiary_map}
    )

    # Execute big query scritp to create stg_contact_details
    create_stg_contacts = bigquery_operator.BigQueryOperator(
        task_id="create_stg_contacts",
        use_legacy_sql=False,
        sql="_sql/stg_contact_details.sql",
        params={
            "project_id": project_id,
            "stg_contact_details": stg_contact_details,
            "contacts": contacts,
            "address_book": address_book,
            "subsidiaries": subsidiaries,
            "companies": companies,
            "entity": entity,
            "contact_types": contact_types}
    )

    # Execute big query scritp to create vendor_edi_856_asn
    create_edi_asn = bigquery_operator.BigQueryOperator(
        task_id="create_edi_asn",
        use_legacy_sql=False,
        sql="_sql/vendor_edi_856_asn.sql",
        params={
            "project_id": project_id,
            "vendor_edi_856_asn": vendor_edi_856_asn,
            "edi_856_asn": edi_856_asn}
    )

    # Execute big query scritp to create vendor_edi_856_asn_lines
    create_edi_asn_lines = bigquery_operator.BigQueryOperator(
        task_id="create_edi_asn_lines",
        use_legacy_sql=False,
        sql="_sql/vendor_edi_856_asn_line.sql",
        params={
            "project_id": project_id,
            "vendor_edi_856_asn_line": vendor_edi_856_asn_line,
            "edi_856_asn_line": edi_856_asn_line,
            "po_types": po_types,
            "merchandise_division": merchandise_division}
    )

    # STEP 2
    # Execute big query scritp to create purchase_order_nested
    create_purchase_orders_nested = bigquery_operator.BigQueryOperator(
        task_id="create_purchase_orders_nested",
        use_legacy_sql=False,
        sql="_sql/purchase_order_nested.sql",
        params={
            "project_id": project_id,
            "purchase_order_nested": purchase_order_nested,
            "purchase_order_all": purchase_order_all,
            "stg_po_lines_nested": stg_po_lines_nested,
            "carrier_shipment_chr_nested": carrier_shipment_chr_nested}
    )

    # Execute big query scritp to create vendor_contacts_nested
    create_vendor_contacts_nested = bigquery_operator.BigQueryOperator(
        task_id="create_vendor_contacts_nested",
        use_legacy_sql=False,
        sql="_sql/vendor_contacts_nested.sql",
        params={
            "project_id": project_id,
            "vendor_contacts_nested": vendor_contacts_nested,
            "stg_contact_details": stg_contact_details,
            "stg_vendor_contacts": stg_vendor_contacts}
    )

    [create_stg_purchase_orders, create_stg_po_lines, create_stg_carrier] >> create_purchase_orders_nested
    [create_stg_vendors, create_stg_contacts] >> create_vendor_contacts_nested
    [create_edi_asn, create_edi_asn_lines]

