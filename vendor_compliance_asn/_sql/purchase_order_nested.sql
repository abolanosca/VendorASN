--drop table elcap_stg_dev.purchase_order_nested 
--create or replace table elcap_dev.purchase_order_nested 
create or replace table {{params.project_id}}.{{params.purchase_order_nested}}
PARTITION BY (expected_arrival_date)
cluster by po_number as
select 
     --cast(lines.expected_arrival_date as date) as expected_arrival_date
     expected_arrival_date
    ,po.po_number
    ,struct(
         po.po_title --
        ,po.po_type --
        ,po.transaction_extid --
        ,po.external_id --
        ,po.memo--
        ,po.buyer as buyer_name--
        ,po.po_status --
        ,po.po_terms--
        ,po.down_payment --
        ,po_email_sent          ---**NO VC
        ,po.po_is_finalized--
        ,po.po_allocation_channel--
        ,po.merchandise_division--
        ,po.location_code--
        ,po.integration_status --
        ,po.po_first_transmission--
        ,po.ready_for_860_transmit --REVIEW
        ,po.record_source_system ---**NO VC
        ,po.po_category--
        ,po.location--
        ,po.po_batch_name--
        ----NEW
        ,po.purchase_order_change_status--
        ,po.reason_for_purchase--
        ,po.competing_bids--
        ,po.cost_center--
        ,po.transmitted_po--
        ,po.sent_to_chr    
        --
        ,struct (
                po.po_created_date,
                po.po_date,--           -----REVISAR
                po.po_cancel_date,--
                po.po_ship_date,--
                po.po_original_ship_date,--
                po.po_original_cancel_date,--
                po.po_due_date,--
                po.po_fixed_payment_date,--
                po.po_edit_date,--
                po.days_to_edit,--
                po.po_edit_fiscal_week_number,--
                po.n_860_last_execution_timestamp, --NO VC -Review
                po.n_850_last_execution_timestamp,--
                po.days_past_due,--  
                po.transaction_purpose
        ) as po_dates
        ,struct (
                po.vendor_id as po_vendor_id--
                ,po.po_vendor--
        ) as po_vendor
        ,struct (
                po.season_code,--
                po.season_year--
        ) as po_season
        ,struct (
                po.acknowledgment_date,--
                po.acknowledgment_terms,--
                po.acknowledgment_scheduled_date,--
                po.acknowledgment_memo,--
                acknowledgment_type --
        ) as acknowledgment
        ,struct (
                po.line_count,--
                po.total_quantity,--
                po.remaining_open_quantity,--
                po.received_amount,--
                po.discount_amount,
                po.reserved_amount,
                po.shipping_amount,
                po.amount_change,
                po.amount_unbilled,
                po.refunded_amount 
        ) as po_amounts
        ,lines.po_lines
    ) as purchase_oder
    ,struct (
         lines.total_on_order_cost
        ,lines.total_on_order_quantity
        ,po.total__amount as ni_submitted_total
        ,lines.total_amount_lines
        ,po.total_amount_billed
        ,po.total_principal
        ,po.total_qv_
        ,po.total_approved
    ) as totals
    ,struct(
         carrier.carrier_shipping_data
        ,carrier.shipment_details
  ) as carrier_shipment_chr
  ,current_timestamp()          as created_date
  ,cast(null as timestamp)      as updated_date
from  
    --elcap_stg_dev.stg_purchase_orders_all po
    {{params.project_id}}.{{params.purchase_order_all}} po
left join
    --elcap_stg_dev.stg_po_lines_nested lines
    {{params.project_id}}.{{params.stg_po_lines_nested}} lines
    on po.po_number=lines.po_number
left join
    --elcap_stg_dev.carrier_shipment_chr_nested carrier
    {{params.project_id}}.{{params.carrier_shipment_chr_nested}} carrier
    on po.po_number=carrier.carrier_shipping_data
--where po.po_number ='12208846711'