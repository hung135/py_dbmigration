DO $$
BEGIN
create schema if not exists test;
create schema if not exists logging;
 
CREATE ROLE operational_dba WITH NOLOGIN;
EXCEPTION WHEN DUPLICATE_OBJECT THEN
RAISE NOTICE 'not creating role -- it already exists';

CREATE TABLE if not exists logging.meta_source_files
(
    id integer NOT NULL DEFAULT nextval('logging.meta_source_files_id_seq'::regclass),
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

TABLESPACE pg_default;

ALTER TABLE logging.meta_source_files
    OWNER to docker;
END
$$;
 