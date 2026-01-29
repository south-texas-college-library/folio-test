-- Get total amount of checkouts from each item record

SELECT
    it.id AS id,
    string_agg(jsonb_extract_path_text(object, 'note'), ', ') AS notes,
    string_agg(jsonb_extract_path_text(object, 'note'), ', ') FILTER (WHERE nt.name = 'Staff Note') AS staff_notes
FROM folio_inventory.item it
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(it.jsonb, 'notes')) AS object ON TRUE
JOIN folio_inventory.item_note_type__t nt ON nt.id = (jsonb_extract_path_text(object, 'itemNoteTypeId'))::uuid
GROUP BY
    it.id