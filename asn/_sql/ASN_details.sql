create or replace table {{params.asn_details_table}} as

select
         entity.name                      	as vendor
        ,brand.brand_name                	as brand
        ,po.trandate                   	        as po_date
        ,po.ship_date
        ,po.cancel_date
        ,po.tranid                         	as po_number
        ,merch.merchandise_division_name   	as merchandise_division
        ,type.list_item_name               	as po_type
        ,season.list_item_name             	as season_code
        ,year.list_item_name               	as season_year
        ,employ .full_name                	as buyer 
        ,lines.transaction_line_id              as line_id
        ,item.item_extid                        as sku 
        ,lines.vendor_sku        
        ,item.upc_code                          as upc 
        ,lines.memo
        ,lines.expected_arrival_eta_line                                                
        ,lines.item_count                       as quantity_in_transaction_unit
        ,lines.original_transmitted_qty                                
        ,lines.quantity_received_in_shipment	as quantity_fulfilled_received
        ,lines.number_billed                    as quantity_billed
        ,(lines.asn_quantity_shipped - lines.quantity_received_in_shipment)	as qty_in_transit
        ,uom.name                               as units
        ,lines.item_unit_price                  as rate
        ,lines.discount_amount 
        ,lines.purchase_price 
        ,lines.amount 
        ,lines.asn_record 
        ,lines.asn_quantity_shipped 
        ,lines.asn_shipment_date 
        ,lines.asn_estimated_delivery_date 
        ,po.n_860_last_execution_timestamp 	as n_860_sent
        ,po.status 
        ,CASE WHEN current_date <=
                (
                        CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN cast(lines.asn_estimated_delivery_date as date)+14 
                        ELSE cast(lines.asn_shipment_date as date) + 14 END 
                ) THEN 'Yes' ELSE 'No' END                          as in_transit_window
        ,CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN  cast(lines.asn_estimated_delivery_date as date) + 14 
        ELSE  cast(lines.asn_shipment_date as date) + 14 END         as transit_window_end
from 
        {{params.ns_transactions}}.transactions po
left join
        {{params.ns_transaction_lines}}.transaction_lines lines ON cast(po.transaction_id as int64) = cast(lines.transaction_id as int64)
left join
        {{params.ns_sc}}.season season ON cast(po.season_code_id as int64) = cast(season.list_id as int64)
left join 
        {{params.ns_sc}}.year1 year ON cast(po.season_year_id as int64) = cast(year.list_id as int64)
left join 
        {{params.ns_sc}}.po_types type ON cast(po.po_type_id as int64) = cast(type.list_id as int64)
left join 
        {{params.ns_entity}}.entity entity ON cast(po.entity_id as int64) = cast(entity.entity_id as int64)
left join 
        {{params.ns_2}}.brand brand ON cast(brand.brand_id as int64) = cast(lines.brand_id as int64)
left join
        {{params.ns_sc}}.merchandise_division merch ON cast(merch.merchandise_division_id as int64) = cast(po.merchandise_division_id as int64)
left join  
        {{params.ns_2}}.employees employ ON cast(po.sales_rep_id as int64) = cast(employ.employee_id as int64)
left join
        {{params.ns_items}}.items_cv item ON cast(lines.item_id as int64) = cast(item.item_id as int64)
left join
        {{params.ns_sc}}.uom uom ON cast(item.uom_id as int64) = cast(uom.uom_id as int64)
left join
        {{params.ns_vendor_asn}}.edi_document_type_status edi ON cast(entity.edi_856_id as int64) = cast(edi.list_id as int64)
left join
        {{params.ns_sc}}.po_types ON cast(po.po_type_id as int64) = cast(po_types.list_id as int64)
where  
        po.transaction_type ='Purchase Order' 
        and transaction_line_id > 0    
        and po_types.list_item_name <> 'Non-Inventory'
        and lines.asn_quantity_shipped > lines.quantity_received_in_shipment 
        and lines.asn_record is not null       
        and edi.list_item_name = 'Yes'
        and current_date <= (
                CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN  cast(lines.asn_estimated_delivery_date as date) + 14 
                ELSE  cast(lines.asn_shipment_date as date) + 14 END)
order by po.tranid asc
;