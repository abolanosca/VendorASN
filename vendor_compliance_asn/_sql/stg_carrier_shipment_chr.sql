--create or replace table elcap_stg_dev.carrier_shipment_chr_nested as
create or replace table {{params.project_id}}.{{params.carrier_shipment_chr_nested}} as
with carrier_cte as
(
    SELECT
        po.tranid AS po_number,
        entity.name as vendor,
        CONCAT('Purchase Order #', ' ', po.tranid) AS custom_po_number,
        nc.po_shipping_status,
        nc.date_created,
        ncs.last_modified_date as last_modified,
        CASE nc.is_inactive WHEN 'T'THEN 'Yes'
            ELSE 'No'
        END as inactive,
        po.tranid AS carrier_shipping_data,
        ncs.last_updated as last_updated,
        ncs.shipment_id as shipment_id,
        ncs.status,
        ncs.delivery_date as delivery_date,
        ncs.tracking_number,
        ncs.carrier_tracking_link_url,
        ncs.tracking_number as tracking_number_html,
        ncs.original_delivery_eta,
        ncs.pick_up_date,
        ncs.location_0 as location,
        ncs.pallet_count,
        ncs.declared_value,
        ncs.shipping_cost,
        ncs.invoice_number,
        CASE ncs.invoiced WHEN 'T'THEN 'Yes'
            ELSE 'No'
        END as invoiced
    FROM 

        {{params.project_id}}.{{params.transactions}} po
    LEFT JOIN 

        {{params.project_id}}.{{params.locations}} lo ON po.location_id = lo.location_id
    LEFT JOIN

        {{params.project_id}}.{{params.transaction_lines}} lines ON cast(po.transaction_id as int64) = cast(lines.transaction_id as int64)
    LEFT JOIN 

        {{params.project_id}}.{{params.carrier_shipping_data}} nc ON nc.carrier_shipping_data_name = po.tranid
    LEFT JOIN 

        {{params.project_id}}.{{params.carrier_shipments}} ncs ON ncs.carrier_shipping_data_id = nc.carrier_shipping_data_id
    left join 

        {{params.project_id}}.{{params.entity}} entity ON cast(po.entity_id as int64) = cast(entity.entity_id as int64)
    WHERE
        po.transaction_type ='Purchase Order'
        and  nc.is_inactive = 'F'
        and po._fivetran_deleted=False and lo._fivetran_deleted=False and lines._fivetran_deleted=False and entity._fivetran_deleted=False

    GROUP BY
        po.tranid
        ,entity.name
        ,nc.po_shipping_status
        ,nc.date_created 
        ,ncs.last_modified_date
        ,nc.is_inactive 
        ,ncs.last_updated 
        ,ncs.shipment_id 
        ,ncs.status
        ,ncs.delivery_date
        ,ncs.tracking_number
        ,ncs.carrier_tracking_link_url
        ,ncs.original_delivery_eta 
        ,ncs.pick_up_date
        ,ncs.location_0 
        ,ncs.pallet_count
        ,ncs.declared_value
        ,ncs.shipping_cost
        ,ncs.invoice_number
        ,ncs.invoiced
)

select 
     carrier_shipping_data
    ,po_number
    ,vendor
    ,custom_po_number
    ,ARRAY_AGG(
      struct(
         shipment_id 		
        ,pallet_count 			
        ,declared_value 			
        ,shipping_cost 			
        ,inactive       
        ,struct (
            tracking_number 
            ,carrier_tracking_link_url 			
            ,location 			
            ,po_shipping_status 	
            ,status
        ) as tracking
        ,struct (
             invoice_number
            ,invoiced
        ) as invoice
        ,struct (
             format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (date_created))    as date_created			
            ,format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (last_modified))   as last_modified			
            ,format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (last_updated))    as last_updated			
            ,format_timestamp("%m-%d-%Y", timestamp (delivery_date))            as delivery_date			
            ,format_timestamp("%m-%d-%Y", timestamp (original_delivery_eta))    as original_delivery_eta			
            ,format_timestamp("%m-%d-%Y", timestamp (pick_up_date))             as pick_up_date
        ) as shipment_dates
      )
  ) shipment_details
from
    carrier_cte
group by 
    po_number,
    vendor,
    custom_po_number,
    carrier_shipping_data