-- Deploy tables:tables/logging.meta_source_files to pg
BEGIN;
create table if not exists logging.meta_source_files (
	id serial not null, 
    project_name character varying COLLATE pg_catalog."default" NOT NULL,
    file_name character varying COLLATE pg_catalog."default" NOT NULL,
    file_path character varying COLLATE pg_catalog."default" NOT NULL,
     
    file_name_data character varying COLLATE pg_catalog."default",
    file_type character varying COLLATE pg_catalog."default",
    file_process_state character varying COLLATE pg_catalog."default",
    process_start_dtm timestamp without time zone,
    process_end_dtm timestamp without time zone,
    current_worker_host character varying COLLATE pg_catalog."default",
    current_worker_host_pid integer,
    rows_inserted bigint,
    data_appended character varying COLLATE pg_catalog."default",
    file_size bigint,
    total_rows bigint,
    total_files integer,
    total_files_processed integer,
    last_error_msg character varying COLLATE pg_catalog."default",
    database_table character varying COLLATE pg_catalog."default",
    parent_file_id integer,
    crc uuid,
    upsert_time timestamp without time zone,
    publish_time timestamp without time zone,
    upsert_function_name character varying COLLATE pg_catalog."default",
    process_msg_trail character varying COLLATE pg_catalog."default" DEFAULT ''::character varying,
    reprocess boolean NOT NULL DEFAULT true,
    duplicate_file boolean NOT NULL DEFAULT false, 
	primary key (id), 
	constraint uq_uniq_files unique (file_name, file_path)
)

;
ALTER TABLE logging.meta_source_files
	OWNER TO operational_dba;
COMMIT;

-- ALTER TABLE logging.meta_source_files
--  ADD COLUMN crc uuid;