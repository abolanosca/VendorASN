create or replace table {{params.edi_chr_po_vendor_fields_table}} as

select
po.tranid,
cast(po.original_ship_date as date) original_ship_date,
cast(po.original_cancel_date as date) original_cancel_date,
po.tranid as Carrier_Shipping_Data,
CASE po.sent_to_chr WHEN  'T'THEN 'Yes'
      ELSE 'No'
END as sent_to_chr,
FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.n_860_last_execution_timestamp)) as n_860_last_execution_timestamp,
cast (po.location_code as INT64) as location_code,
ist.list_item_name as integration_status_id,
rf.list_item_name as ready_for_860_transmit_id,
FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.n_850_last_execution_timestamp)) as n_850_last_execution_timestamp,
FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (po.acknowledgment_date)) as acknowledgment_date,
po.acknowledgment_terms,
po.acknowledgment_scheduled_date,
po.acknowledgment_memo,
atyp.list_item_name as acknowledgment_type_id,
status.list_item_name as acknowledgment_item_status,
lines.acknowledgment_rate,
lines.acknowledgment_scheduled_date as acknowledgment_scheduled_date1,
lines.acknowledgment_scheduled_quan,
po.acknowledgment_memo as acknowledgment_memo1,
lines.n_2nd_acknowledgment_item_s_id as n2nd_Acknowledgment_Item_Status,
lines.n_2nd_acknowledgment_scheduled as n_2nd_acknowledgment_scheduled_date,
lines.n_2nd_acknowledgment_schedul_0 as n_2nd_acknowledgment_scheduled_quantity,
lines.asn_record,
lines.asn_quantity_shipped,
lines.asn_shipment_date,
lines.asn_estimated_delivery_date,
CASE vendor.edi WHEN  'T'THEN 'Yes'
      ELSE 'No'
END as edi
,edi810.list_item_name      as edi_810 
,edi846.list_item_name      as edi_846 
,edi850.list_item_name      as edi_850 
,edi855.list_item_name      as edi_855 
,FORMAT_TIMESTAMP("%m-%d-%Y", TIMESTAMP (vendor.edi_855_activation_date)) as edi_855_activation_date
,edi856.list_item_name      as edi_856 
,edi860.list_item_name      as edi_860 
,CASE vendor.edi_capable WHEN  'T'THEN 'Yes'
      ELSE 'No'
END as edi_capable

from {{params.ns_transactions}}.transactions po
left join {{params.ns_transaction_lines}}.transaction_lines lines ON cast(po.transaction_id as int64) = cast(lines.transaction_id as int64)
left join {{params.ns_vendor_asn}}.acknowledgment_item_statuses status ON lines.acknowledgment_item_status_id = status.list_id
left join {{params.ns_vendor_asn}}.acknowledgment_types atyp ON atyp.list_id= status.list_id
left join {{params.ns_vendor_asn}}.integrationstatuses ist ON po.integration_status_id = ist.list_id
left join {{params.ns_vendor_asn}}.ready_for_860_transmit rf ON ist.list_id = rf.list_id
left join {{params.ns_2}}.vendors vendor  ON vendor.currency_id = po.currency_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi850 ON edi850.list_id = edi_850_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi860 ON edi860.list_id=edi_860_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi855 ON edi855.list_id=edi_855_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi856 ON edi856.list_id=edi_856_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi810 ON edi810.list_id=edi_810_id
left join {{params.ns_vendor_asn}}.edi_document_type_status edi846 ON edi846.list_id=edi_846_id
where po.transaction_type ='Purchase Order'
and vendor.edi= 'T'
and po.n_850_last_execution_timestamp >= DATE_ADD(current_timestamp(), INTERVAL -30 DAY) 
group by
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
order by 3,32 desc;