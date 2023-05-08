create or replace table {{params.project_id}}.{{params.stg_po_lines_nested}}
cluster by po_number as 
with po_lines_cte as (
select 
   po.tranid as po_number
   ,max(po.transaction_id)    as transaction_id
    ,item.item_extid                --as sku
    ,lines.vendor_sku               --as vendor_sku
    ,item.salesdescription    as item_description
    ,lines.memo as po_item_description
    ,cast(ifnull(lines.expected_arrival_eta_line,(CAST('1900-01-01' AS TIMESTAMP))) as date) as expected_arrival_eta_line
    ,period_closed as closed
    ,lines.item_count               --as quantity
    ,lines.original_transmitted_qty 
    ,lines.quantity_received_in_shipment --as received
    ,lines.number_billed            --as billed
    ,uom.name AS unit_of_measure
    ,uom.conversion_rate
    ,lines.item_unit_price          --as rate
    ,REGEXP_REPLACE(lines.item_unit_price, r'\%|-', '')  AS rate --------
    ,lines.purchase_price
    ,lines.discount_amount          
    ,lines.amount
    ,pgroup.product_group_name
    ,item.upc_code                          
    ,brand.brand_name 
    ,item.axis_1_option_header_id as options
    ,lines.line_created_date
    ,lines.quantity_available
    ,ack_status.list_item_name    as acknowledgment_item_status
    ,lines.transaction_line_id
    ,case 
            when inv.sku is null then false
            else true
    end  AS inventory_owned_flag
    ,current_timestamp() as transaction_expected_arrival_date
    ,lines.acknowledgment_rate
    ,format_timestamp("%m-%d-%Y",timestamp(lines.acknowledgment_scheduled_date)) as acknowledgment_scheduled_date
    ,lines.acknowledgment_scheduled_quan as acknowledgment_scheduled_quantity
    ,lines.acknowledgment_memo
    ,n2_ack_status.list_item_name         as n_2nd_acknowledgment_item_status
    ,lines.n_2nd_acknowledgment_scheduled as n_2nd_acknowledgment_scheduled_date
    ,lines.n_2nd_acknowledgment_schedul_0 as n_2nd_acknowledgment_schedul_quantity
    ,lines.asn_record
    ,lines.asn_quantity_shipped
    ,format_timestamp("%m-%d-%Y",timestamp(lines.asn_shipment_date))            as asn_shipment_date
    ,format_timestamp("%m-%d-%Y",timestamp(lines.asn_estimated_delivery_date))  as asn_estimated_delivery_date
    ,lines.demand_amount
    ,lines.demand_cogs
    ,lines.demand_quantity
    ,lines.estimated_cost
    ,lines.full_price
    ,lines.shipping_amount
    ,lines.amount_custom
    ,lines.amount_foreign
    ,lines.amount_pending
    ,lines.original_cogs
    ,lines.po__rate
    ,lines.quantity_allocated
    ,lines.quantity_custom
    ,lines.quantity_difference
    ,lines.quantity_on_order
    ,lines.rate_custom
    ,ifnull(((lines.item_count)/uom.conversion_rate),0)      as quantity_in_transaction_unit
    , CASE WHEN (lines.quantity_received_in_shipment/uom.conversion_rate) < coalesce(lines.asn_quantity_shipped, 0) THEN 
        case when  current_date <= (
          CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN cast(lines.asn_estimated_delivery_date as date) + 14 
          ELSE cast(lines.asn_shipment_date as date) + 14 END 
        ) then (lines.asn_quantity_shipped - (lines.quantity_received_in_shipment/uom.conversion_rate))
        end
      ELSE null END as qty_in_transit
    ---
    ,CASE WHEN current_date <=
      (
        CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN cast(lines.asn_estimated_delivery_date as date) + 14 
          ELSE cast(lines.asn_shipment_date as date) + 14 END 
      ) THEN 'Yes' ELSE 'No' END                          as in_transit_window

    ,CASE WHEN current_date <=
      (
        CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN cast(lines.asn_estimated_delivery_date as date) + 14 
          ELSE cast(lines.asn_shipment_date as date) + 14 END 
      ) THEN 
          format_timestamp("%m-%d-%Y",timestamp(CASE WHEN cast(lines.asn_estimated_delivery_date as date) + 14 is not Null THEN  cast(lines.asn_estimated_delivery_date as date) + 14 
          ELSE  cast(lines.asn_shipment_date as date) + 14 END         
        )) 
      ELSE null END                          as transit_window_end
    -------
    ,SUM(ifnull(lines.item_count,0) - ifnull(receipts.received_quantity,0)) AS on_order_quantity ------------ REVIEW
    ,SUM((ifnull(lines.item_count,0) - ifnull(receipts.received_quantity,0)) * CAST(ifnull(replace(lines.item_unit_price,'%',''),"0") AS float64)) AS on_order_cost------------ REVIEW
    ,SUM(ifnull(lines.item_count,0) - ifnull(sum(receipts.received_quantity),0)) OVER (PARTITION BY po.tranid) AS total_on_order_quantity
    ,SUM((ifnull(lines.item_count,0) - ifnull(sum(receipts.received_quantity),0)) * CAST(ifnull(replace(lines.item_unit_price,'%',''),"0") AS float64)) OVER (PARTITION BY po.tranid) AS total_on_order_cost
    ,SUM(ifnull(lines.amount,0)) OVER (PARTITION BY po.tranid) AS total_amount_lines 
from
  {{params.project_id}}.{{params.transactions}} po
INNER JOIN
  {{params.project_id}}.{{params.transaction_lines}} lines ON lines.transaction_id = po.transaction_id
LEFT JOIN
  {{params.project_id}}.{{params.items_cv}} item ON  cast(lines.item_id as int64) = cast(item.item_id as int64)
LEFT JOIN 
  (
        SELECT distinct sku
        FROM {{params.project_id}}.{{params.sb_inventory}}
        WHERE snapshot_day = timestamp_sub(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, DAY, 'America/Denver'), interval 1 day) 
  ) inv ON inv.sku = item.item_extid
LEFT JOIN
  {{params.project_id}}.{{params.uom}} uom ON cast(lines.unit_of_measure_id as int64) = cast(uom.uom_id as int64)
left join
  {{params.project_id}}.{{params.brand}} brand ON cast(brand.brand_id as int64) = cast(lines.brand_id as int64)
left join
  {{params.project_id}}.{{params.acknowledgment_item_statuses}} ack_status on ack_status.list_id=lines.acknowledgment_item_status_id
left join
  {{params.project_id}}.{{params.acknowledgment_item_statuses}} n2_ack_status on n2_ack_status.list_id=lines.n_2nd_acknowledgment_item_s_id
left join
  {{params.project_id}}.{{params.product_group}} pgroup ON pgroup.product_group_id=lines.product_group_id
LEFT JOIN (
  SELECT
    f.po_number,
    f.received_sku,
    f.RECORD_SOURCE,
    f.po_purchase_order_item_id,
    SUM(f.received_quantity) received_quantity
    FROM
      {{params.project_id}}.{{params.sb_receipts}} f
    GROUP BY
      f.po_number,
      f.received_sku,
      f.RECORD_SOURCE,
      f.po_purchase_order_item_id
    ) receipts ON receipts.received_sku = item.item_extid AND receipts.po_number = po.tranid 
    and receipts.po_purchase_order_item_id = lines.transaction_line_id
WHERE
    po.tranid IS NOT NULL and po.transaction_type = 'Purchase Order'
    and lines.transaction_line_id >= 1
    and po._fivetran_deleted=False and lines._fivetran_deleted=False and pgroup._fivetran_deleted=False and brand._fivetran_deleted=False

group by 
  po.tranid
    ,item.item_extid
    ,lines.vendor_sku
    ,item.salesdescription
    ,lines.memo
    ,lines.expected_arrival_eta_line
    ,period_closed
    ,lines.item_count
    ,lines.original_transmitted_qty 
    ,lines.quantity_received_in_shipment
    ,lines.number_billed
    ,uom.name 
    ,uom.conversion_rate
    ,lines.item_unit_price 
    ,lines.purchase_price
    ,lines.discount_amount          
    ,lines.amount
    ,pgroup.product_group_name
    ,item.upc_code                          
    ,brand.brand_name 
    ,item.axis_1_option_header_id
    ,lines.line_created_date
    ,lines.quantity_available
    ,ack_status.list_item_name
    ,lines.transaction_line_id
    ,inv.sku
    ,lines.acknowledgment_rate
    ,lines.acknowledgment_scheduled_date
    ,lines.acknowledgment_scheduled_quan
    ,lines.acknowledgment_memo
    ,n2_ack_status.list_item_name
    ,lines.n_2nd_acknowledgment_scheduled
    ,lines.n_2nd_acknowledgment_schedul_0
    ,lines.asn_record
    ,lines.asn_quantity_shipped
    ,lines.asn_shipment_date
    ,lines.asn_estimated_delivery_date
    ,lines.demand_amount
    ,lines.demand_cogs
    ,lines.demand_quantity
    ,lines.estimated_cost
    ,lines.full_price
    ,lines.shipping_amount
    ,lines.amount_custom
    ,lines.amount_foreign
    ,lines.amount_pending
    ,lines.original_cogs
    ,lines.po__rate
    ,lines.quantity_allocated
    ,lines.quantity_custom
    ,lines.quantity_difference
    ,lines.quantity_on_order
    ,lines.rate_custom

)
select 
      po_number,
      ARRAY_AGG( STRUCT(
      item_extid,
      vendor_sku,
      item_description,
      po_item_description,
      expected_arrival_eta_line,
      closed,
      item_count,
      original_transmitted_qty,
      quantity_received_in_shipment,
      number_billed,
      unit_of_measure,
      conversion_rate,
      item_unit_price,
      rate,
      purchase_price,
      discount_amount,
      amount,
      product_group_name,
      upc_code,
      brand_name,
      options,
      line_created_date,
      quantity_available,
      acknowledgment_item_status,
      transaction_line_id,
      inventory_owned_flag,
      transaction_expected_arrival_date,
      acknowledgment_rate,
      acknowledgment_scheduled_date,
      acknowledgment_scheduled_quantity,
      acknowledgment_memo,
      n_2nd_acknowledgment_item_status,
      n_2nd_acknowledgment_scheduled_date,
      n_2nd_acknowledgment_schedul_quantity,
      asn_record,
      asn_quantity_shipped,
      asn_shipment_date,
      asn_estimated_delivery_date,
      demand_amount,
      demand_cogs,
      demand_quantity,
      estimated_cost,
      full_price,
      shipping_amount,
      amount_custom,
      amount_foreign,
      amount_pending,
      original_cogs,
      po__rate,
      quantity_allocated,
      quantity_custom,
      quantity_difference,
      quantity_on_order,
      rate_custom,
      quantity_in_transaction_unit,
      in_transit_window,------------------
      transit_window_end,
      qty_in_transit,
      on_order_quantity,
      on_order_cost
      ) ) as po_lines
      ,max(max(total_on_order_quantity)) OVER (PARTITION BY po_number)            AS total_on_order_quantity
      ,max(max(total_on_order_cost)) OVER (PARTITION BY po_number)                AS total_on_order_cost
      ,max(max(expected_arrival_eta_line)) OVER (PARTITION BY po_number)          AS expected_arrival_date
      ,round(max(max(total_amount_lines)) OVER (PARTITION BY po_number),2)        AS total_amount_lines
    from
      po_lines_cte lines
    group by po_number