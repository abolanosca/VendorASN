create or replace table {{params.asn_summary_table}} as


with po_details as
(
    select 
        merchandise_division, assigned_to_id, assigned_to
	    ,count(distinct po_number) over (partition by merchandise_division ) as count_orders
	    ,count(distinct vendor) over (partition by merchandise_division ) count_vendors
    from(
            select 
                entity.name 								as vendor
                ,po.tranid 									as po_number
                ,merch2.merchandise_division_name	        as merchandise_division
                ,assigned.full_name                         as assigned_to
                ,merch2.assigned_to_id  
                ,lines.transaction_line_id		            as line_id			
                ,lines.item_count 						    as quantity_in_transaction_unit
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
                {{params.ns_sc}}.merchandise_division merch2 ON cast(merch2.merchandise_division_id as int64) = cast(po.merchandise_division_id as int64)
            left join 
                {{params.ns_items}}.items_cv item ON cast(lines.item_id as int64) = cast(item.item_id as int64)

            left join
                {{params.ns_sc}}.uom uom ON cast(item.uom_id as int64) = cast(uom.uom_id as int64)
            left join
                {{params.ns_2}}.employees assigned ON assigned.employee_id=merch2.assigned_to_id
            left join
                {{params.ns_vendor_asn}}.edi_document_type_status edi ON cast(entity.edi_856_id as int64) = cast(edi.list_id as int64)
            left join
                {{params.ns_sc}}.po_types ON cast(po.po_type_id as int64) = cast(po_types.list_id as int64)
            where  
            po.transaction_type ='Purchase Order' 
            and transaction_line_id > 0
            and po_types.list_item_name <> 'Non-Inventory'
            and lines.asn_quantity_shipped > lines.item_count 
            and lines.asn_record is not null  
            and edi.list_item_name = 'Yes'         
            and current_date <= (
                CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN  cast(lines.asn_estimated_delivery_date as date) + 14 
                ELSE  cast(lines.asn_shipment_date as date) + 14 END)
        )
)
select 
	 merchandise_division
	,max(count_vendors) as count_vendors
	,max(count_orders) as count_orders
    ,assigned_to
	,count(*) as count_problems_lines
from po_details
group by merchandise_division	, assigned_to_id, assigned_to 
order by merchandise_division asc