--create or replace table elcap_dev.vendor_edi_856_asn_line
create or replace table {{params.project_id}}.{{params.vendor_edi_856_asn_line}}
cluster by po_number as
SELECT
  line.edi_856_asn_line_id, --
  line.asn_vendor_id,
  carrier_package_id,--
  format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (line.date_created)) as date_created,
  line.edi_856_asn_id,
  line.edi_856_asn_line_extid,
  format_timestamp("%m-%d-%Y", timestamp (line.estimated_delivery_date)) as estimated_delivery_date,
  case when line.is_inactive = 'F' then False
    else True
  end as is_inactive,
  format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (line.last_modified_date)) as last_modified_date,
  line.line_number,--
  line.line_ship_quantity,--
  line.mpn,--
  line.order_qty_uom,--
  line.pack_level_type,--
  line.pack_value,--
  line.pack_weight,--
  line.pack_weight_uom,--
  line.packing_material,
  line.packing_medium,
  line.parent_id,
  line.po_md_id,--
  merch.merchandise_division_name,
  line.po_type_id,--
  type.list_item_name               	as po_type,
  line.product_description,--
  format_timestamp("%m-%d-%Y", timestamp (line.purchase_order_date)) as purchase_order_date,--
  line.purchase_order_id,
  line.purchase_order_id_0  as po_number,
  line.quantity_ordered     as po_internal_id,--
  line.ship_qty_uom,--
  format_timestamp("%m-%d-%Y", timestamp (line.shipment_date)) as shipment_date,
  line.shipping_serial_id,--
  line.sku,
  line.upc,
  line.vendor_part_number--
FROM
  {{params.project_id}}.{{params.edi_856_asn_line}} line
left join

  {{params.project_id}}.{{params.po_types}} type ON cast(line.po_type_id as int64) = cast(type.list_id as int64)
left join 

  {{params.project_id}}.{{params.merchandise_division}} merch ON cast(merch.merchandise_division_id as int64) = cast(line.po_md_id as int64)
WHERE line._fivetran_deleted=False and type._fivetran_deleted=False and merch._fivetran_deleted=False
