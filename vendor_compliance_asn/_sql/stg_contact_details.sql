--create  or replace table  elcap_stg_dev.stg_contact_details 
create  or replace table  {{params.project_id}}.{{params.stg_contact_details}} 
cluster by company_name as
select 
  contacts.contact_id
  ,contacts.full_name

  ,contacts.name                                    as contact
  ,contacts.salutation                              as salutation_MR_MS
  ,contacts.firstname || ' ' || contacts.lastname   as name
  ,contacts.company_id
  ,companies.companyname                            as company_name
  ,contacts.title                                   as job_title
  ,contacts.comments
  ,contacts.is_private                              as private
  ,ARRAY_AGG(
    coalesce(contact_types.name,'')
  )                                                 as category

  ,contacts.email
  ,contacts.altemail
  ,contacts.phone                                   as mainphone
  ,contacts.officephone
  ,contacts.mobilephone
  ,contacts.homephone
  ,contacts.fax 
  ,contacts.address
  ,contacts.subsidiary                              as subsidiary_id
  ,sub.name                                         as subsidiary
  ,contacts.last_sales_activity
  ,sup.name                                         as supervisor
  ,contacts.supervisorphone
  ,assist.name                                      as assistand
  ,contacts.assistantphone
  ,address_book.is_default_ship_address
  ,address_book.is_default_bill_address
  ,address_book.address                 as address_tab
  ,contacts.isinactive
from 

  {{params.project_id}}.{{params.contacts}}
left join 

  {{params.project_id}}.{{params.address_book}} ON address_book.entity_id=contacts.contact_id
left join

  {{params.project_id}}.{{params.subsidiaries}} sub ON sub.subsidiary_id=contacts.subsidiary
left join

  {{params.project_id}}.{{params.companies}} ON companies.company_id=contacts.company_id
left join

  {{params.project_id}}.{{params.entity}} sup ON sup.entity_id=contacts.supervisior_id
left join

  {{params.project_id}}.{{params.entity}} assist ON assist.entity_id=contacts.assistant_id
left join

  {{params.project_id}}.{{params.contact_types}} ON contact_types.contact_id=contacts.contact_id 
where contacts._fivetran_deleted = False and contacts.company_id is not null and contacts.isinactive='No'
group by 
  contacts.contact_id
  ,contacts.full_name
  ,contacts.name
  ,contacts.salutation
  ,contacts.firstname
  ,contacts.lastname
  ,contacts.company_id
  ,companies.companyname
  ,contacts.title
  ,contacts.comments
  ,contacts.is_private
  ,contacts.email
  ,contacts.altemail
  ,contacts.phone
  ,contacts.officephone
  ,contacts.mobilephone
  ,contacts.homephone
  ,contacts.fax 
  ,contacts.address
  ,contacts.subsidiary
  ,sub.name
  ,contacts.last_sales_activity
  ,sup.name
  ,contacts.supervisorphone
  ,assist.name
  ,contacts.assistantphone
  ,address_book.is_default_ship_address
  ,address_book.is_default_bill_address
  ,address_book.address
  ,contacts.isinactive