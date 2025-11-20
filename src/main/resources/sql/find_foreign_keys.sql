SELECT     tc.constraint_name,
           tc.table_name,
           kcu.column_name,
           ccu.table_name AS referenced_table,
           ccu.column_name AS referenced_column
FROM information_schema.table_constraints AS tc
         JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
         JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.constraint_schema = 'hmpps_court_case_service'
ORDER BY tc.table_name