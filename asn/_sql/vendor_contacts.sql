create or replace table {{params.vendor_contacts_table}}
as
with general as
(
      select 
          vendor.name
          ---Primary information
          ,vendor.companyname
          ,case when vendor.isinactive = 'No' then 'Active'
                  else 'Inactive'
          end                 as status
          ,vendor.name        as vendor_id
          ,vendor.vendor_id          as internal_vendor_id
          ,case when vendor.is_person = 'No' then 'Company'
                  else 'Individual'
          end                 as type
          ,vtype.name         as vendor_category
          ,pterms.name        as terms
          ,vendor.incoterm
          ,vendor.comments
          ---Email, phone, address
          ,vendor.email
          ,vendor.email_address_for_payment_not
          ,vendor.accounting_email
          ,vendor.phone
          ,vendor.altphone
          ,vendor.fax
          ,vendor.url         as web_address
          ,vendor.billaddress as address
      from 
        {{params.ns_2}}.vendors vendor
      left join 
        {{params.ns_sc}}.vendor_types vtype ON vendor.vendor_type_id = vtype.vendor_type_id
      left join
        {{params.ns_sc}}.payment_terms pterms ON vendor.payment_terms_id = pterms.payment_terms_id
      left join  
        {{params.ns_2}}.employees employee ON cast(vendor.created_by_ven_id as int64) = cast(employee.employee_id as int64)
      where vendor.companyname is not null 
),
address as (
      select 
         vendor.name
        ,vendor.vendor_id
        -----Address Tab
        ,ARRAY_AGG
        (
          struct(   address.is_default_bill_address
                ,address.is_default_ship_address
                ,address.name   as label
                ,address.address
                ,address.is_inactive
          )   
        )  as address_tab
      from 
        {{params.ns_2}}.vendors vendor
      left join
        {{params.ns_2}}.address_book address ON vendor.vendor_id=address.entity_id
      where companyname is not null 
      group by vendor.name,vendor.vendor_id
),
purchasing as (
      select 
        vendor.name
        ,vendor.vendor_id
        ----Purchasing Tab
        ,struct(
        vendor.edi
        ,vendor.edi_855_activation_date
        ,edi850.list_item_name      as edi_850 
        ,edi860.list_item_name      as edi_860 
        ,edi855.list_item_name      as edi_855 
        ,edi856.list_item_name      as edi_856 
        ,edi810.list_item_name      as edi_810 
        ,edi846.list_item_name      as edi_846 
        ,vendor.edi_capable
        ,vendor.creditlimit
        ,vendor.vendor_credit_limit_date
        ,currency.name as currency
        ) as purchasing_tab
    from 
      {{params.ns_2}}.vendors vendor
    left join 
      {{params.ns_sc}}.vendor_types vtype ON vendor.vendor_type_id = vtype.vendor_type_id
    left join
      {{params.ns_sc}}.currencies currency ON vendor.currency_id = currency.currency_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi850 ON edi850.list_id=edi_850_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi860 ON edi860.list_id=edi_860_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi855 ON edi855.list_id=edi_855_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi856 ON edi856.list_id=edi_856_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi810 ON edi810.list_id=edi_810_id
    left join
      {{params.ns_vendor_asn}}.edi_document_type_status edi846 ON edi846.list_id=edi_846_id
    where companyname is not null 
),
financials as (
      select 
        vendor.name
        ,vendor.vendor_id
        ----Financials
        ,struct(
        abs(vs_map.balance) as balance
        ,abs(vs_map.unbilled_orders) as unbilled_orders
        ,currency.name as currency
        ) as financials_tab
    from 
      {{params.ns_2}}.vendors vendor
    left join
      {{params.ns_sc}}.currencies currency ON vendor.currency_id = currency.currency_id
    left join
      {{params.ns_vendor_asn}}.vendor_subsidiary_map vs_map ON  vs_map.vendor_id=vendor.vendor_id
    where companyname is not null 
),
relationships as (
    select 
          vendor.name                as vendor
          ,vendor.vendor_id          
          ---Comunications
          ,ARRAY_AGG(
            struct(
              contacts.name             as contact_name
              ,type.name                as category
              ,contacts.title            as job_title
              ,contacts.email
              ,contacts.phone
              ,contacts.contact_id 
              )
          )     as relationships_tab
      from 
        {{params.ns_2}}.vendors vendor
      left join
        {{params.ns_vendor_asn}}.contacts ON contacts.company_id=vendor.vendor_id
      left join
        {{params.ns_vendor_asn}}.contact_types type on contacts.contact_id=type.contact_id
      where companyname is not null 
      group by 1,2
),
communications as (
        select 
          vendor.name                as vendor
          ,vendor.vendor_id
          ---Comunications
          ,ARRAY_AGG(
            struct(
              message.date_0           as date
              ,message.author_id
              ,author.full_name      as author
              ,message.recipient_id
              ,coalesce(recipient.full_name,message.to_0) as primary_recipient
              ,message.subject
              ,message.to_0
              ,message.from_0
              ,message.message_type_id
              ,message.message  
            )
          )      as communications_tab 
      from 
        {{params.ns_2}}.vendors vendor
      left join
        {{params.ns_vendor_asn}}.message ON message.company_id=vendor.vendor_id
      left join 
         {{params.ns_2}}.employees	author ON message.author_id=author.employee_id
      left join 
         {{params.ns_2}}.employees	recipient ON message.recipient_id=recipient.employee_id
      where vendor.companyname is not null 
      group by 1,2
),
approvals as (
      select 
          vendor.name
          ,vendor.vendor_id 
          ----Approvals
          ,ARRAY_AGG(
            struct(
              vendor.date_created
              ,employee.full_name     as created_by
              ,finance_status.list_item_name  as finance_status
              ,operations_status.list_item_name as operations_status
            )
          ) as approvals_tab
      from 
         {{params.ns_2}}.vendors vendor
      left join  
         {{params.ns_2}}.employees employee ON cast(vendor.created_by_ven_id as int64) = cast(employee.employee_id as int64)
      left join
        {{params.ns_vendor_asn}}.finance_status ON cast(vendor.finance_status_ven_id as int64) = cast(finance_status.list_id as int64)
      left join
        {{params.ns_vendor_asn}}.operations_status ON cast(vendor.operations_status_ven_id as int64) = cast(operations_status.list_id as int64)
      group by 1,2 
),
subsidiaries as (
        select 
          vendor.name
          ,vendor.vendor_id
          -------subsidiaries
            ,ARRAY_AGG( 
                struct( 
                        sub.name  as primary_subsidiary
                        ,ven_sub.name as subsidiary
                        ,case when vs_map.subsidiary_id = vendor.subsidiary then True
                              else False
                        end as primary
                        ,ven_sub.isinactive
                        ,abs(vs_map.balance) as balance
                        ,abs(vs_map.balance_base) as balance_base
                        ,abs(vs_map.unbilled_orders) as unbilled_orders
                        ,abs(vs_map.unbilled_orders_base) as unbilled_orders_base
                        ,vs_map.credit_limit
                        ,vs_map.subsidiary_id
                        ,currency.name as currency
                        ,ven_sub.state_tax_number as tax_code
                        )
            )  as subsidiaries_tab
      from 
         {{params.ns_2}}.vendors vendor
      left join
         {{params.ns_sc}}.currencies currency ON vendor.currency_id = currency.currency_id
      left join
         {{params.ns_sc}}.subsidiaries sub ON vendor.subsidiary=sub.subsidiary_id
      left join
        {{params.ns_vendor_asn}}.vendor_subsidiary_map vs_map ON  vs_map.vendor_id=vendor.vendor_id
      left join 
        {{params.ns_sc}}.subsidiaries ven_sub ON ven_sub.subsidiary_id=sub.subsidiary_id
      where companyname is not null 
      group by 1,2
)
select 
  general.*
  ,address.address_tab
  ,purchasing.purchasing_tab
  ,financials.financials_tab
  ,relationships.relationships_tab
  ,communications.communications_tab
  ,approvals.approvals_tab
  ,subsidiaries.subsidiaries_tab
from 
  general
left join
  address ON general.internal_vendor_id=address.vendor_id
left join
  purchasing ON general.internal_vendor_id=purchasing.vendor_id
left join
  financials ON general.internal_vendor_id=financials.vendor_id
left join
  relationships ON general.internal_vendor_id=relationships.vendor_id
left join
  communications ON general.internal_vendor_id=communications.vendor_id
left join
  approvals ON general.internal_vendor_id=approvals.vendor_id
left join 
  subsidiaries ON general.internal_vendor_id=subsidiaries.vendor_id