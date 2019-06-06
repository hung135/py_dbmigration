CREATE or replace VIEW logging.finished_imports as  SELECT DISTINCT p.id,
    p.file_name_data,
    p.file_name,
  
    c.last_error_msg
   FROM logging.meta_source_files p
     LEFT JOIN logging.meta_source_files c ON c.parent_file_id = p.id AND c.file_process_state::text <> 'PROCESSED'::text and  c.duplicate_file is False
  WHERE p.parent_file_id = 0 AND p.file_process_state::text = 'PROCESSED'::text AND c.id IS NULL;
GRANT ALL ON TABLE logging.finished_imports TO operational_dba;