CREATE OR REPLACE TABLE
  --elcap_dev.vendor_contacts_nested AS
  {{params.project_id}}.{{params.vendor_contacts_nested}} AS
WITH
  contact_cte AS (
    SELECT
        company_id,
        ARRAY_AGG( STRUCT( contact_id,
            full_name,
            contact,
            salutation_MR_MS,
            name AS contact_name,
            company_id,
            company_name,
            job_title,
            comments,
            private,
            category,
            email,
            altemail,
            mainphone,
            officephone,
            mobilephone,
            homephone,
            fax,
            address,
            subsidiary_id,
            subsidiary,
            last_sales_activity,
            supervisor,
            supervisorphone,
            assistand,
            assistantphone,
            is_default_ship_address,
            is_default_bill_address,
            address_tab,
            isinactive ) ) contacts
    FROM
        --elcap_dev.contact_details -> stg_contact_details
        {{params.project_id}}.{{params.stg_contact_details}}
    WHERE
        company_id IS NOT NULL
        AND isinactive='No'
    GROUP BY company_id )
SELECT
  vendor.name,
  vendor.companyname,
  vendor.status,
  vendor.vendor_id,
  vendor.internal_vendor_id,
  vendor.custom_vendor_externalid,
  vendor.type,
  vendor.vendor_category,
  vendor.terms,
  vendor.incoterm,
  vendor.comments,
  STRUCT ( 
    vendor.email,
    vendor.email_address_for_payment_not,
    vendor.accounting_email,
    vendor.phone,
    vendor.altphone,
    vendor.fax,
    vendor.web_address,
    vendor.address 
  ) AS email_phone_address, 
  vendor.address_tab,
  vendor.purchasing_tab,
  vendor.financials_tab,
  vendor.relationships_tab,
  vendor.communications_tab,
  vendor.approvals_tab,
  vendor.subsidiaries_tab,
  contact_cte.contacts
FROM
  --elcap_stg_dev.stg_vendor_contacts vendor
  {{params.project_id}}.{{params.stg_vendor_contacts}} vendor
LEFT JOIN
  contact_cte
ON
  vendor.internal_vendor_id=contact_cte.company_id