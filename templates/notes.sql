-- Get total amount of checkouts from each item record

SELECT
    it.id AS item_id,
    string_agg(jsonb_extract_path_text(item_note.element, 'note'), ', ') AS notes,
    string_agg(jsonb_extract_path_text(item_note.element, 'note'), ', ') FILTER (WHERE nt.name = 'Staff Note') AS staff_notes
FROM folio_inventory.item it
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(it.jsonb, 'notes')) AS item_note (element)
JOIN folio_inventory.item_note_type__t nt ON nt.id = (jsonb_extract_path_text(item_note.element, 'itemNoteTypeId'))::uuid
GROUP BY
    it.id