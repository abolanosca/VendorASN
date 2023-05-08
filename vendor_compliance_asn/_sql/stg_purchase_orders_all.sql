--create  or replace table  elcap_stg_dev.stg_purchase_orders_all
create  or replace table  {{params.project_id}}.{{params.purchase_order_all}} 
cluster by po_number as
select distinct
      po.tranid                                                 AS po_number
    , po.po_batch_name                                          AS po_title
    , ty.list_item_name                                         AS po_type
    , po.transaction_extid                                      --AS po_extid
    , po.external_id
    --, po.memo                                                   AS po_notes
    ,em.full_name                                               AS buyer 
    , po.status                                                 AS po_status
    , en.name                                                   AS po_vendor
    , pt.name                                                   AS po_terms
    --, po.cancel_date                                            AS po_cancel_date
    ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.cancel_date))   AS po_cancel_date
    --, po.trandate                                               AS po_created_date
    ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.trandate  ))   AS po_created_date
    --, po.ship_date                                              AS po_ship_date
    ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.ship_date  ))   AS po_ship_date
    , 
    FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (
    cast(case 
       when  lower(po.status) IN (
        'partially received'                    
        , 'pending billing/partially received'  
        , 'pending receipt'                     
        , 'pending bill'                        
        , 'fully billed'                        
        ) then po.ship_date  
       else  CAST('1900-01-01' as timestamp)   end   as date) )) AS  po_original_ship_date
    , FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (
    cast(case 
       when  lower(po.status) IN (
        'partially received'                    
        , 'pending billing/partially received'  
        , 'pending receipt'                     
        , 'pending bill'                        
        , 'fully billed'                        
        ) then po.original_cancel_date
       else  CAST('1900-01-01' as timestamp)   end    as date) ))   AS po_original_cancel_date
    , po.due_date                                               AS po_due_date
    , '-1'                                                      AS po_email_sent
    , CASE po.first_transmission 
        WHEN 'T' THEN 'True' 
        WHEN 'F' THEN 'False' 
        ELSE '' 
    END                                                         AS po_first_transmission
    ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.fixed_payment_date)) as po_fixed_payment_date
    , CASE po.is_finalized 
        WHEN 'T' THEN 'True' 
        WHEN 'F' THEN 'False' 
        ELSE '' 
    END                                                         AS po_is_finalized 
    , 'NS'                                                      AS record_source_system
    , FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (
    TIMESTAMP_SUB(po.ship_date , interval cast(coalesce(po.days_to_edit,0) as int64) DAY) )) as po_edit_date
   -- FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.ship_date  ))   AS po_ship_date
    , dd.REALIGNED_FISCAL_WEEK_NO                                     AS po_edit_fiscal_week_number
        ,po.days_to_edit
        ,alc.list_item_name AS po_allocation_channel
       -- cast(tlpo.item_count as int64) AS po_original_quantity
        , po.entity_id   AS vendor_id
        --, po.trandate    AS po_date
        ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.trandate)) AS po_date
        ,merch.merchandise_division_name    AS merchandise_division
        ,s.list_item_name                   AS season_code
        ,y.list_item_name                   AS season_year
        ,FORMAT_TIMESTAMP("%m-%d-%Y %H:%M:%S", TIMESTAMP (po.n_860_last_execution_timestamp)) as n_860_last_execution_timestamp
        --FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.n_860_last_execution_timestamp)) as n_860_last_execution_timestamp
        ,FORMAT_TIMESTAMP("%m-%d-%Y %H:%M:%S", TIMESTAMP (po.n_850_last_execution_timestamp)) as n_850_last_execution_timestamp
        ,po.discount_amount
        ,po.down_payment
        ,po.reserved_amount
        ,po.shipping_amount
        ,po.amount_change
        ,po.amount_unbilled
        ,po.total__amount
        ,po.total_amount_billed
        ,po.total_principal
        ,po.total_qv_
        ,po.total_approved
        ,po.refunded_amount
        ,pc.list_item_name as po_category
        ,l.full_name as location
        ,po.memo
        ,po.po_batch_name
        ,count(tlpo.transaction_line_id) as line_count
        ,sum(tlpo.item_count) as total_quantity
        ,sum(tlpo.item_count - tlpo.quantity_received_in_shipment) as remaining_open_quantity
        ,case
       when po.due_date is not null  
        then DATE_DIFF(current_date, cast(po.due_date as date),day)  
       else 0 end as days_past_due
       ,po.due_date
       --,sum(cast(tlpo.quantity_received_in_shipment as float64) * cast(tlpo.item_unit_price as float64)) as received_amount
       , SUM((ifnull(cast(tlpo.quantity_received_in_shipment as float64), 0) * ifnull(cast(replace(tlpo.item_unit_price,'%','') AS float64), 0))) AS received_amount
        , CASE po.transmitted_po
            WHEN 'T' THEN 'True' 
            WHEN 'F' THEN 'False' 
            ELSE '' 
        END as transmitted_po 
        ,dept.full_name as cost_center
        ,l.location_code
        ,ist.list_item_name as integration_status
        ,po.competing_bids
        ,po.reason_for_purchase
        ,pocs.list_item_name as purchase_order_change_status 
        ,rf.list_item_name as ready_for_860_transmit 
        ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.acknowledgment_date)) as acknowledgment_date
        ,po.acknowledgment_terms 
        ,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.acknowledgment_scheduled_date)) as acknowledgment_scheduled_date 
        ,po.acknowledgment_memo 
        ,ack_types.list_item_name as acknowledgment_type
        ,po.transaction_purpose
        ,CASE po.sent_to_chr WHEN  'T'THEN 'Yes'
            ELSE 'No'
        END as sent_to_chr,
FROM 
    --ns_transactions.transactions po
    {{params.project_id}}.{{params.transactions}} po
    --LEFT JOIN netsuite2.employees em ON po.sales_rep_id = em.employee_id
    LEFT JOIN 
        {{params.project_id}}.{{params.employees}} em ON po.sales_rep_id = em.employee_id
    --LEFT JOIN ns_entity.entity en ON cast(po.entity_id as int64) = cast(en.entity_id as int64)
    LEFT JOIN 
        {{params.project_id}}.{{params.entity}} en ON cast(po.entity_id as int64) = cast(en.entity_id as int64)
    --LEFT JOIN netsuite_sc.po_types ty ON cast(po.po_type_id as int64) = cast(ty.list_id as int64)
    LEFT JOIN 
        {{params.project_id}}.{{params.po_types}} ty ON cast(po.po_type_id as int64) = cast(ty.list_id as int64)
    --LEFT JOIN netsuite_sc.season s ON cast(po.season_code_id as int64) = cast(s.list_id as int64)
    LEFT JOIN 
        {{params.project_id}}.{{params.season}} s ON cast(po.season_code_id as int64) = cast(s.list_id as int64)
    --LEFT JOIN netsuite_sc.year1 y ON cast(po.season_year_id as int64) = cast(y.list_id as int64)
    LEFT JOIN 
        {{params.project_id}}.{{params.year}} y ON cast(po.season_year_id as int64) = cast(y.list_id as int64)
    --LEFT JOIN netsuite_sc.payment_terms pt ON cast(po.payment_terms_id as int64) = cast(pt.payment_terms_id as int64)
    LEFT JOIN 
        {{params.project_id}}.{{params.payment_terms}} pt ON cast(po.payment_terms_id as int64) = cast(pt.payment_terms_id as int64)
    --LEFT JOIN elcap.dim_dates dd on TIMESTAMP_TRUNC(dd.calendar_date,DAY) = TIMESTAMP_SUB(po.ship_date , interval cast(coalesce(po.days_to_edit,0) as int64)     DAY)
    LEFT JOIN 
        {{params.project_id}}.{{params.dim_dates}} dd on TIMESTAMP_TRUNC(dd.calendar_date,DAY) = TIMESTAMP_SUB(po.ship_date , interval cast(coalesce(po.days_to_edit,0) as int64)     DAY)
    LEFT JOIN 
        {{params.project_id}}.{{params.dim_dates}} dc on TIMESTAMP_TRUNC(dc.calendar_date,DAY) = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as TIMESTAMP) 
    --LEFT JOIN netsuite2.allocation_channel alc on po.allocation_channel_id = alc.list_id
    LEFT JOIN 
        {{params.project_id}}.{{params.allocation_channel}} alc on po.allocation_channel_id = alc.list_id
    --INNER JOIN ns_transaction_lines.transaction_lines tlpo ON tlpo.transaction_id = po.transaction_id
    INNER JOIN 
        {{params.project_id}}.{{params.transaction_lines}} tlpo ON tlpo.transaction_id = po.transaction_id
    --LEFT JOIN  netsuite_sc.merchandise_division merch ON cast(merch.merchandise_division_id as int64) = cast(po.merchandise_division_id as int64)
    LEFT JOIN  
        {{params.project_id}}.{{params.merchandise_division}} merch ON cast(merch.merchandise_division_id as int64) = cast(po.merchandise_division_id as int64)
    --LEFT JOIN  netsuite_sc.locations l ON l.location_id = po.location_id
    LEFT JOIN  
        {{params.project_id}}.{{params.locations}} l ON l.location_id = po.location_id
    --LEFT JOIN netsuite_vendor_asn.integrationstatuses ist ON po.integration_status_id = ist.list_id
    LEFT JOIN 
        {{params.project_id}}.{{params.integrationstatuses}} ist ON po.integration_status_id = ist.list_id
    --LEFT JOIN  netsuite_sc.po_category pc ON pc.list_id = po.po_category_id
    LEFT JOIN  
        {{params.project_id}}.{{params.po_category}} pc ON pc.list_id = po.po_category_id
    --LEFT JOIN netsuite_vendor_asn.ready_for_860_transmit rf ON po.ready_for_860_transmit_id = rf.list_id 
    LEFT JOIN 
        {{params.project_id}}.{{params.ready_for_860_transmit}} rf ON po.ready_for_860_transmit_id = rf.list_id 
    --LEFT JOIN  fivetran_transaction_lines_temp.purchase_order_change_status  pocs ON pocs.list_id = po.purchase_order_change_statu_id
    LEFT JOIN  
        {{params.project_id}}.{{params.purchase_order_change_status}}  pocs ON pocs.list_id = po.purchase_order_change_statu_id
    --LEFT JOIN netsuite_sc.departments dept on dept.department_id = tlpo.department_id 
    LEFT JOIN 
        {{params.project_id}}.{{params.departments}} dept on dept.department_id = tlpo.department_id  
    --left join netsuite_vendor_asn.acknowledgment_types ack_types on  po.acknowledgment_type_id=ack_types.list_id
    left join 
        {{params.project_id}}.{{params.acknowledgment_types}} ack_types on  po.acknowledgment_type_id=ack_types.list_id
WHERE
    po.tranid IS NOT NULL and po.transaction_type = 'Purchase Order'
    and po._fivetran_deleted=False and em._fivetran_deleted=False and en._fivetran_deleted=False and tlpo._fivetran_deleted=False
    --and po.tranid = '12215395525'
    and tlpo.transaction_line_id >= 1
    group by 
      po.tranid                                                 
    , po.po_batch_name                                         
    , s.list_item_name                                        
    , y.list_item_name                                        
    , ty.list_item_name                                        
    , ty.list_item_name                                         
    , po.transaction_extid
    , po.external_id                                     
    , po.memo                                                  
    , po.status                                                
    , em.full_name                                              
    , en.name                                                   
    , pt.name                                                   
    , po.cancel_date                                            
    , po.trandate                                               
    , po.ship_date                                              
    , po_original_ship_date
    --, po_original_cancel_date
    , po.original_cancel_date
    , po.days_to_edit                                           
    , po.due_date                                             
    , po.down_payment                                         
    , po_email_sent
    , po_fixed_payment_date
    , po_is_finalized 
    , record_source_system
    , po_edit_date
    , po_edit_fiscal_week_number
    ,po_allocation_channel
    ,po.entity_id
    ,merch.merchandise_division_name
    ,s.list_item_name
    ,y.list_item_name 
    ,em.full_name
    ,po.n_860_last_execution_timestamp
    ,po.n_850_last_execution_timestamp
    ,po.discount_amount
    ,po.reserved_amount
    ,po.shipping_amount 
    ,po.amount_change
    ,po.amount_unbilled
    ,po.total__amount
    ,po.total_amount_billed
    ,po.total_principal
    ,po.total_qv_
    ,po.total_approved
    ,po.refunded_amount
    ,l.full_name
    ,pc.list_item_name 
    ,po.first_transmission 
    ,l.location_code
    ,ist.list_item_name 
    ,rf.list_item_name
    ,po.transmitted_po
    ,competing_bids
    ,reason_for_purchase
    ,pocs.list_item_name
    ,dept.full_name
    ,po.acknowledgment_date
    ,po.acknowledgment_terms 
    ,po.acknowledgment_scheduled_date 
    ,po.acknowledgment_memo
    ,ack_types.list_item_name
    ,po.transaction_purpose
    ,po.sent_to_chr  
;