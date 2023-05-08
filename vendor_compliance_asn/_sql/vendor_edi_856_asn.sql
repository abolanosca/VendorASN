--create or replace table elcap_dev.vendor_edi_856_asn
create or replace table {{params.project_id}}.{{params.vendor_edi_856_asn}}
cluster by edi_856_asn_name as
SELECT
  edi_856_asn_id, --
  asn_vendor_id,--
  bill_of_lading_number,--
  carrier_alpha_code,--
  carrier_equipment_number,--
  carrier_pro_number,--
  carrier_routing,--
  carrier_trans_method_code,--
  format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (date_created)) as date_created,--
  --format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (date_deleted)) as date_deleted,
  edi_856_asn_extid,
  edi_856_asn_file,--
  edi_856_asn_name,
  equipment_description_code,--
  format_timestamp("%m-%d-%Y", timestamp (estimated_delivery_date)) as estimated_delivery_date,--
  fob_pay_code,--
  case when is_inactive = 'F' then False
    else True
  end as is_inactive,
  lading_quantity,--
  format_timestamp("%m-%d-%Y %H:%M:%S", timestamp (last_modified_date)) as last_modified_date,
  packing_material,--
  packing_medium,--
  parent_id,
  ship_from_address,--
  ship_from_address_1,
  ship_from_address_2,
  ship_from_address_3,
  ship_from_address_4,
  ship_from_address_name,
  ship_from_city,
  ship_from_country,
  ship_from_postal_code,
  ship_from_state,
  ship_to_address,--
  ship_to_address_1,
  ship_to_address_2,
  ship_to_address_3,
  ship_to_address_4,
  ship_to_address_name,
  ship_to_city,
  ship_to_country,
  ship_to_postal_code,
  ship_to_state,--
  format_timestamp("%m-%d-%Y", timestamp (shipment_date)) as shipment_date,--
  shipment_id,--
  format_timestamp("%m-%d-%Y", timestamp (shipment_notice_date)) as shipment_notice_date,--
  shipment_notice_time,--
  status_code,--
  total_line_item_number,--
  tset_purpose_code,--
  vendor_idname_id,
  weight,--
  weight_qualifier,--
  weight_uom--
FROM
  --netsuite_vendor_asn.edi_856_asn
  {{params.project_id}}.{{params.edi_856_asn}}
where _fivetran_deleted=False
--and edi_856_asn_id=281902