-- metadb:function lib_cat_activity_tracker_1

DROP FUNCTION IF EXISTS lib_cat_activity_tracker_1(date, date, text);

CREATE FUNCTION lib_cat_activity_tracker_1(
    start_date date DEFAULT DATE '2000-01-01',
    end_date   date DEFAULT DATE '2050-01-01',
    system     text DEFAULT NULL
)
RETURNS TABLE(
    cataloger text,
    username text,
    period_type text,
    period_start date,
    period_end date,
    instance_created numeric,
    field_added numeric,
    field_modified numeric,
    field_removed numeric,
    item_added numeric,
    item_updated numeric,
    item_withdrawn numeric
)
AS $$
WITH RECURSIVE
parameters AS (
    SELECT
        start_date AS report_start,
        end_date   AS report_end,
        CASE
            /*
             * Check month first because any date range longer than
             * one month is also longer than one week.
             */
            WHEN end_date > (start_date + INTERVAL '1 month')::date
                THEN 'month'

            WHEN end_date > start_date + 7
                THEN 'week'

            ELSE 'total'
        END AS period_type
),

/*
 * Generate every reporting period, even if no activity occurred
 * during one of the periods.
 */
reporting_periods AS (
    /* Weekly and monthly periods */
    SELECT
        p.period_type,
        gs::date AS bucket_start,
        GREATEST(gs::date, p.report_start) AS period_start,
        LEAST(
            CASE p.period_type
                WHEN 'month'
                    THEN (gs + INTERVAL '1 month - 1 day')::date
                WHEN 'week'
                    THEN (gs + INTERVAL '6 days')::date
            END,
            p.report_end
        ) AS period_end
    FROM parameters p
    CROSS JOIN LATERAL generate_series(
        date_trunc(p.period_type, p.report_start::timestamp),
        date_trunc(p.period_type, p.report_end::timestamp),
        CASE p.period_type
            WHEN 'month' THEN INTERVAL '1 month'
            WHEN 'week'  THEN INTERVAL '1 week'
        END
    ) AS gs
    WHERE p.period_type IN ('month', 'week')

    UNION ALL

    /* One general-total period */
    SELECT
        p.period_type,
        p.report_start AS bucket_start,
        p.report_start AS period_start,
        p.report_end AS period_end
    FROM parameters p
    WHERE p.period_type = 'total'
),

catalogers (username, cataloger, display_order) AS (
    VALUES
        ('ahern267', 'boomer',   1),
        ('treyna1',  'helo',     2),
        ('mtorre43', 'apollo',   3),
        ('marjona4', 'starbuck', 4),
        (NULL,       'husker',   5)
),

/*
 * Normalize Inventory activity into one result set.
 *
 * metric identifies which output column will receive the count.
 */
inventory_activity AS (

    /* Instances created */
    SELECT
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'createdDate'
            )::timestamp
        )::date
    END AS bucket_start,

        COALESCE(c.cataloger, 'husker') AS cataloger,
        'instance_created'::text AS metric,
        COUNT(*)::numeric AS activity_count

    FROM folio_inventory.instance__ i

    CROSS JOIN parameters p

    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid =
           jsonb_extract_path_text(
               i.jsonb,
               'metadata',
               'createdByUserId'
           )::uuid

       AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(
                COALESCE(
                    pu.jsonb -> 'permissions',
                    '[]'::jsonb
                )
            ) permission
            WHERE permission =
                '07e78044-2804-496e-a3e7-074f557dd361'
       )

    LEFT JOIN folio_users.users__t created_by
        ON created_by.id =
           jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid

    LEFT JOIN catalogers c
        ON c.username = created_by.username

    WHERE jsonb_extract_path_text(i.jsonb, 'hrid')
              !~ '^(SE|L|RSV|T)'

      AND jsonb_extract_path_text(
              i.jsonb,
              'metadata',
              'createdDate'
          )::date
          BETWEEN p.report_start AND p.report_end

    GROUP BY
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'createdDate'
            )::timestamp
        )::date
    END,
        COALESCE(c.cataloger, 'husker')

    UNION ALL

    /* Items created */
    SELECT
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'createdDate'
            )::timestamp
        )::date
    END AS bucket_start,

        COALESCE(c.cataloger, 'husker') AS cataloger,
        'item_added'::text AS metric,
        COUNT(*)::numeric AS activity_count

    FROM folio_inventory.item__ i

    CROSS JOIN parameters p

    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid =
           jsonb_extract_path_text(
               i.jsonb,
               'metadata',
               'createdByUserId'
           )::uuid

       AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(
                COALESCE(
                    pu.jsonb -> 'permissions',
                    '[]'::jsonb
                )
            ) permission
            WHERE permission =
                '07e78044-2804-496e-a3e7-074f557dd361'
       )

    LEFT JOIN folio_users.users__t created_by
        ON created_by.id =
           jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid

    LEFT JOIN catalogers c
        ON c.username = created_by.username

    WHERE jsonb_extract_path_text(i.jsonb, 'barcode')
              !~ '^(SE|L|RSV|T)'

      AND jsonb_extract_path_text(
              i.jsonb,
              'metadata',
              'createdDate'
          )::date
          BETWEEN p.report_start AND p.report_end

    GROUP BY
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'createdDate'
            )::timestamp
        )::date
    END,
        COALESCE(c.cataloger, 'husker')

    UNION ALL

    /* Items updated */
    SELECT
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE    date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'updatedDate'
            )::timestamp
        )::date
    END AS bucket_start,

        COALESCE(c.cataloger, 'husker') AS cataloger,
        'item_updated'::text AS metric,
        COUNT(*)::numeric AS activity_count

    FROM folio_inventory.item__ i

    CROSS JOIN parameters p

    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid =
           jsonb_extract_path_text(
               i.jsonb,
               'metadata',
               'updatedByUserId'
           )::uuid

       AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(
                COALESCE(
                    pu.jsonb -> 'permissions',
                    '[]'::jsonb
                )
            ) permission
            WHERE permission =
                '07e78044-2804-496e-a3e7-074f557dd361'
       )

    LEFT JOIN folio_users.users__t updated_by
        ON updated_by.id =
           jsonb_extract_path_text(
               pu.jsonb,
               'userId'
           )::uuid

    LEFT JOIN catalogers c
        ON c.username = updated_by.username

    WHERE jsonb_extract_path_text(i.jsonb, 'barcode')
              !~ '^(SE|L|RSV|T)'

      AND jsonb_extract_path_text(
              i.jsonb,
              'metadata',
              'updatedDate'
          )::date
          BETWEEN p.report_start AND p.report_end

    GROUP BY
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'metadata',
                'updatedDate'
            )::timestamp
        )::date
    END,
        COALESCE(c.cataloger, 'husker')

    UNION ALL

    /* Items withdrawn */
    SELECT
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'status',
                'date'
            )::timestamp
        )::date
    END AS bucket_start,

        COALESCE(c.cataloger, 'husker') AS cataloger,
        'item_withdrawn'::text AS metric,
        COUNT(*)::numeric AS activity_count

    FROM folio_inventory.item__ i

    CROSS JOIN parameters p

    LEFT JOIN folio_users.users__t updated_by
        ON updated_by.id =
           jsonb_extract_path_text(
               i.jsonb,
               'metadata',
               'updatedByUserId'
           )::uuid

    LEFT JOIN catalogers c
        ON c.username = updated_by.username

    WHERE jsonb_extract_path_text(
              i.jsonb,
              'status',
              'name'
          ) = 'Withdrawn'

      AND jsonb_extract_path_text(
              i.jsonb,
              'status',
              'date'
          )::date
          BETWEEN p.report_start AND p.report_end

    GROUP BY
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            jsonb_extract_path_text(
                i.jsonb,
                'status',
                'date'
            )::timestamp
        )::date
    END,
        COALESCE(c.cataloger, 'husker')
),

inventory_summary AS (
    SELECT
        bucket_start,
        cataloger,

        COALESCE(
            SUM(activity_count)
            FILTER (WHERE metric = 'instance_created'),
            0
        )::numeric AS instance_created,

        COALESCE(
            SUM(activity_count)
            FILTER (WHERE metric = 'item_added'),
            0
        )::numeric AS item_added,

        COALESCE(
            SUM(activity_count)
            FILTER (WHERE metric = 'item_updated'),
            0
        )::numeric AS item_updated,

        COALESCE(
            SUM(activity_count)
            FILTER (WHERE metric = 'item_withdrawn'),
            0
        )::numeric AS item_withdrawn

    FROM inventory_activity

    GROUP BY
        bucket_start,
        cataloger
),

audit_data AS (
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p0_2026_q2__
    WHERE action = 'UPDATED'

    /*
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p0_2026_q3__
    WHERE action = 'UPDATED'
    */

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p1_2026_q2__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p1_2026_q3__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p2_2026_q2__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p2_2026_q3__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p3_2026_q2__
    WHERE action = 'UPDATED'

    /*
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p3_2026_q3__
    WHERE action = 'UPDATED'
    */

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p4_2026_q2__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p4_2026_q3__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p5_2026_q2__
    WHERE action = 'UPDATED'

    /*
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p5_2026_q3__
    WHERE action = 'UPDATED'
    */

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p6_2026_q2__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p6_2026_q3__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p7_2026_q2__
    WHERE action = 'UPDATED'

    UNION ALL

    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p7_2026_q3__
    WHERE action = 'UPDATED'
),

field_changes AS (
    SELECT
    CASE
        WHEN p.period_type = 'total'
            THEN p.report_start
        ELSE
        date_trunc(
            p.period_type,
            ad.event_date::timestamp
        )::date
    END AS bucket_start,

        COALESCE(c.cataloger, 'husker') AS cataloger,
        fc ->> 'changeType' AS change_type

    FROM audit_data ad

    CROSS JOIN parameters p

    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(
            ad.diff::jsonb -> 'fieldChanges',
            '[]'::jsonb
        )
    ) AS fc

    LEFT JOIN folio_users.users__t audit_user
        ON audit_user.id = ad.user_id

    LEFT JOIN catalogers c
        ON c.username = audit_user.username

    WHERE ad.event_date::date
          BETWEEN p.report_start AND p.report_end

      AND fc ->> 'changeType' IS NOT NULL
),

marc_summary AS (
    SELECT
        bucket_start,
        cataloger,

        COUNT(*)
        FILTER (WHERE change_type = 'ADDED')
        ::numeric AS field_added,

        COUNT(*)
        FILTER (WHERE change_type = 'MODIFIED')
        ::numeric AS field_modified,

        COUNT(*)
        FILTER (WHERE change_type = 'REMOVED')
        ::numeric AS field_removed

    FROM field_changes

    GROUP BY
        bucket_start,
        cataloger
),

/*
 * Create one row for every period/cataloger combination.
 * This guarantees zero-value rows when no activity occurred.
 */
period_catalogers AS (
    SELECT
        c.cataloger,
        c.username,
        rp.period_type,
        rp.bucket_start,
        rp.period_start::date,
        rp.period_end::date,
        c.display_order

    FROM reporting_periods rp
    CROSS JOIN catalogers c

    WHERE
           system IS NULL
        OR BTRIM(system) = ''
        OR LOWER(BTRIM(system)) = 'all'
        OR LOWER(c.cataloger) = LOWER(BTRIM(system))
)

SELECT
    pc.cataloger,
    pc.username,
    pc.period_type,
    pc.period_start::date,
    pc.period_end::date,

    COALESCE(
        inventory.instance_created,
        0
    )::numeric AS instance_created,

    COALESCE(
        marc.field_added,
        0
    )::numeric AS field_added,

    COALESCE(
        marc.field_modified,
        0
    )::numeric AS field_modified,

    COALESCE(
        marc.field_removed,
        0
    )::numeric AS field_removed,

    COALESCE(
        inventory.item_added,
        0
    )::numeric AS item_added,

    COALESCE(
        inventory.item_updated,
        0
    )::numeric AS item_updated,

    COALESCE(
        inventory.item_withdrawn,
        0
    )::numeric AS item_withdrawn

FROM period_catalogers pc

LEFT JOIN inventory_summary inventory
    ON inventory.bucket_start = pc.bucket_start
   AND inventory.cataloger = pc.cataloger

LEFT JOIN marc_summary marc
    ON marc.bucket_start = pc.bucket_start
   AND marc.cataloger = pc.cataloger

ORDER BY
    pc.period_start,
    pc.display_order;
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;