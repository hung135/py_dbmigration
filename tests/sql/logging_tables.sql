-- Table: logging.meta_source_files

-- DROP TABLE logging.meta_source_files;

CREATE TABLE if not exists logging.meta_source_files
(
    id serial,
    project_name character varying(256) COLLATE pg_catalog."default" NOT NULL,
    file_name character varying(256) COLLATE pg_catalog."default" NOT NULL,
    file_path character varying(256) COLLATE pg_catalog."default" NOT NULL,
    file_name_data character varying(256) COLLATE pg_catalog."default",
    file_type character varying(256) COLLATE pg_catalog."default",
    file_process_state character varying(256) COLLATE pg_catalog."default",
    process_start_dtm timestamp without time zone,
    process_end_dtm timestamp without time zone,
    current_worker_host character varying(256) COLLATE pg_catalog."default",
    current_worker_host_pid integer,
    rows_inserted bigint,
    data_appended character varying(256) COLLATE pg_catalog."default",
    file_size bigint,
    total_rows bigint,
    total_files integer,
    total_files_processed integer,
    last_error_msg character varying(2000) COLLATE pg_catalog."default",
    database_table character varying(256) COLLATE pg_catalog."default",
    parent_file_id integer,
    crc character varying(32) COLLATE pg_catalog."default",
    upsert_time timestamp without time zone,
    publish_time timestamp without time zone,
    upsert_function_name character varying(256) COLLATE pg_catalog."default",
    reprocess boolean,
    process_msg_trail character varying(2000) COLLATE pg_catalog."default",
    duplicate_file boolean,
    CONSTRAINT meta_source_files_pkey PRIMARY KEY (id),
    CONSTRAINT uq_uniq_files UNIQUE (file_name, file_path)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;



CREATE OR REPLACE VIEW logging.finished_imports AS
 SELECT DISTINCT p.id,
    p.file_name_data,
    p.file_name,
    c.last_error_msg
   FROM logging.meta_source_files p
     LEFT JOIN logging.meta_source_files c ON c.parent_file_id = p.id AND c.file_process_state::text <> 'Processed'::text
  WHERE p.parent_file_id = 0 AND p.file_process_state::text = 'Processed'::text AND c.id IS NULL;
