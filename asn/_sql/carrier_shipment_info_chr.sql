create or replace table {{params.carrier_shipment_info_chr_table}} as
SELECT
po.tranid AS name,
entity.name as vendor,
CONCAT('Purchase Order #', ' ', po.tranid) AS po_number,
nc.po_shipping_status,
nc.date_created,
nc.last_modified_date as last_modified,
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
END as invoiced,

FROM {{params.ns_transactions}}.transactions po
LEFT JOIN {{params.ns_sc}}.locations lo ON po.location_id = lo.location_id
LEFT JOIN {{params.ns_transaction_lines}}.transaction_lines lines ON cast(po.transaction_id as int64) = cast(lines.transaction_id as int64)
LEFT JOIN {{params.ns_vendor_asn}}.carrier_shipping_data nc ON nc.carrier_shipping_data_name = po.tranid
LEFT JOIN {{params.ns_vendor_asn}}.carrier_shipments ncs ON ncs.carrier_shipping_data_id = nc.carrier_shipping_data_id
LEFT JOIN {{params.ns_entity}}.entity entity ON cast(po.entity_id as int64) = cast(entity.entity_id as int64)
WHERE
po.transaction_type ='Purchase Order'
and  nc.is_inactive = 'F'
and  nc.po_shipping_status <> "CREATED"
and  ncs.status <> "Pending"
and  nc.date_created >= DATE_ADD(current_timestamp(), INTERVAL -30 DAY)
GROUP BY
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
ORDER BY 2;