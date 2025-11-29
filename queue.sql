-- Event Queue schema and helper routines
--
-- This script defines a simple queuing model with partitioned/rotating datasets
-- for high-throughput ingestion. The Rust `Queue` implementation uses two DB
-- helper functions declared here: `get_current_dataset(queue_id, threshold, incoming_count)`
-- to select/prepare the active dataset tables to write into, and
-- `release_reservation(queue_id, dataset_id, count)` to finalize a reservation
-- after COPY-ing rows. Data is written into tables named
-- `queue_<queue_id>_data_<dataset_id>` and jobs into `queue_<queue_id>_job_<dataset_id>`.
--
-- The script also includes convenience routines for creating datasets and
-- updating statuses for jobs, along with indexes to support scheduling.
--
-- Cleanup
drop table if exists queue;
drop table if exists destination;
do
$$
    declare
        r record;
    begin
        for r in (select tablename
                  from pg_tables
                  where schemaname = 'public'
                    and tablename ~ '^queue_[0-9]+_data_[0-9]+$')
            loop
                execute 'drop table if exists ' || quote_ident(r.tablename) || ' cascade';
            end loop;
    end
$$;
do
$$
    declare
        r record;
    begin
        for r in (select tablename
                  from pg_tables
                  where schemaname = 'public'
                    and tablename ~ '^queue_[0-9]+_job_[0-9]+$')
            loop
                execute 'drop table if exists ' || quote_ident(r.tablename) || ' cascade';
            end loop;
    end
$$;
drop table if exists source;
drop routine if exists get_current_dataset;
drop routine if exists release_reservation;
drop routine if exists add_destination;
drop routine if exists create_dataset;
drop routine if exists update_status;
drop type if exists failed_job_update;
drop type if exists job_status;

-- Setup
-- Main queue metadata table tracking dataset state and reservations
create table queue
(
    -- Unique identifier for each queue
    id                  serial primary key,
    -- Timestamp when the queue was created
    created_at          timestamptz not null default now(),
    -- Timestamp when the queue was last updated
    updated_at          timestamptz not null default now(),
    -- Number of slots reserved for pending insertions (prevents oversizing datasets)
    reserved_slots      int         not null default 0,
    -- Current dataset ID where new data is being written
    current_dataset     int         not null default 0
        constraint queue_current_dataset_check check (current_dataset >= 0),
    -- Dataset ID currently being processed (may lag behind current_dataset)
    processing_dataset  int         not null default 0
        constraint queue_processing_dataset_check check ( processing_dataset >= 0 and processing_dataset <= current_dataset ),
    -- Last dataset that had failed jobs (used for compaction tracking)
    last_failed_dataset int         not null default 0
        constraint queue_last_failed_dataset_check check ( last_failed_dataset >= 0 and last_failed_dataset <= processing_dataset )
);

-- Enum type for job status lifecycle
-- pending: newly inserted, ready to process
-- processing: currently being processed
-- failed: processing failed but will be retried
-- done: successfully processed
-- failed_permanently: exceeded retry limit, no further processing
create type job_status as enum ('pending', 'processing', 'failed', 'done', 'failed_permanently');

-- Function to create a new dataset partition (data and job tables)
-- A dataset is a logical partition for high-throughput event ingestion
-- Each dataset has a size limit (default 100k rows) to maintain performance
create or replace function create_dataset(queue_id int, dataset_id int) returns void as
$$
declare
    data_table_name   text; -- Name for the data table (stores event payloads)
    job_table_name    text; -- Name for the job table (stores processing metadata)
    status_index_name text; -- Index name for job status queries
    try_at_index_name text; -- Index name for scheduling queries
begin
    -- Construct table and index names
    data_table_name := format('queue_%s_data_%s', queue_id, dataset_id);
    job_table_name := format('queue_%s_job_%s', queue_id, dataset_id);
    status_index_name := format('queue_%s_job_%s_status_idx', queue_id, dataset_id);
    try_at_index_name := format('queue_%s_job_%s_try_at_idx', queue_id, dataset_id);

    -- Create data table to store event payloads as JSONB
    -- Each row is identified by a UUID and contains the full event data
    execute format(
            'create table %I (
                id     uuid primary key,
                data   jsonb not null
            )',
            data_table_name);

    -- Create job table to track processing state for each event
    -- Links to data table via UUID foreign key
    execute format(
            'create table %I (
                id         serial primary key,
                status     job_status not null default ''pending'',
                data       uuid not null,
                try_at     timestamptz not null default now(),
                retry_count int not null default 0,
                created_at timestamptz not null default now(),
                updated_at timestamptz not null default now()
            )',
            job_table_name, data_table_name);

    -- Create index on status column for efficient pending job queries
    execute format('create index %I on %I (status)', status_index_name, job_table_name);

    -- Create index on try_at column for efficient scheduling and retry queries
    execute format('create index %I on %I (try_at)', try_at_index_name, job_table_name);
end;
$$ language plpgsql;

-- Function to create a new queue with its initial dataset
-- Returns the queue ID for use in subsequent operations
create or replace function create_queue() returns int as
$$
declare
    queue_id   int; -- ID of the newly created queue
    dataset_id int; -- ID of the initial dataset (always 0)
begin
    -- Insert a new queue row with default values
    insert into queue default values returning id, current_dataset into queue_id, dataset_id;

    -- Create the initial dataset (data and job tables)
    perform create_dataset(queue_id, dataset_id);

    return queue_id;
end;
$$ language plpgsql;

-- Composite type for batch updates of failed jobs
-- Used to efficiently update multiple failed jobs with their retry timestamps
-- Maps to the Rust FailedJobUpdate struct for seamless serialization
create type failed_job_update as
(
    id     int,        -- Job ID to update
    try_at timestamptz -- When the job should be retried
);

-- Function to batch update job statuses after processing
-- Efficiently updates multiple jobs in a single transaction based on processing results
-- Called by the Rust event processor after processing a batch of events
create or replace function update_status(queue_id int, dataset_id int, done_jobs int[],
                                         failed_permanently_jobs int[], failed_jobs failed_job_update[]) returns void as
$$
declare
    table_name text; -- Name of the job table for this dataset
begin
    -- Construct the job table name
    table_name := format('queue_%s_job_%s', queue_id, dataset_id);

    -- Update successfully processed jobs to 'done' status
    if array_length(done_jobs, 1) > 0 then
        execute format('update %I set
                           status = ''done'',
                           updated_at = now()
                       where id = any($1)',
                       table_name) using done_jobs;
    end if;

    -- Update jobs that exceeded retry limit to 'failed_permanently' status
    -- These will not be retried and should be handled by error handling logic
    if array_length(failed_permanently_jobs, 1) > 0 then
        execute format('update %I set
                            status = ''failed_permanently'',
                            updated_at = now()
                       where id = any($1)',
                       table_name) using failed_permanently_jobs;
    end if;

    -- Update jobs that failed but can be retried
    -- Sets status to 'failed' and updates try_at to the calculated retry timestamp
    if array_length(failed_jobs, 1) > 0 then
        execute format(
                'update %I set
                    status = ''failed'',
                    updated_at = now(),
                    try_at = f.try_at
                from unnest($1) as f where %I.id = f.id',
                table_name, table_name) using failed_jobs;
    end if;
end;
$$ language plpgsql;

-- Function to get the current dataset for insertion with automatic rotation
-- This function manages dataset size limits and automatically creates new datasets
-- when the current one is approaching capacity
-- Returns the dataset ID where data should be inserted
create or replace function get_current_dataset(queue_id int, expected_insertion_size int, max_events_per_dataset int) returns int as
$$
declare
    threshold              int; -- Maximum size per dataset (100k rows)
    dataset                int; -- Current dataset ID
    current_reserved_slots int; -- Slots already reserved for pending inserts
    job_table_name         text; -- Name of the job table for current dataset
    count                  int; -- Current row count in job table
begin
    -- Set maximum partition size to 100k rows to maintain query performance
    threshold := max_events_per_dataset;

    -- Validate that the requested insertion size doesn't exceed threshold
    if expected_insertion_size > threshold then
        raise exception 'expected_insertion_size must be less than %', threshold;
    end if;

    -- Lock the queue row and get current dataset and reserved slots
    -- FOR UPDATE ensures no concurrent modifications during dataset rotation
    select queue.current_dataset, queue.reserved_slots
    into dataset, current_reserved_slots
    from queue
    where id = queue_id for update;

    -- Count existing jobs in the current dataset
    job_table_name := format('queue_%s_job_%s', queue_id, dataset);
    execute format('select count(*) from %I', job_table_name) into count;

    -- Check if inserting would exceed threshold (considering existing + reserved + new)
    if count >= threshold - expected_insertion_size - current_reserved_slots then
        -- Current dataset is full, rotate to a new dataset
        dataset := dataset + 1;
        update queue
        set current_dataset = dataset,
            reserved_slots  = expected_insertion_size
        where id = queue_id;
        -- Create the new dataset tables
        perform create_dataset(queue_id, dataset);
    else
        -- Current dataset has capacity, just increment reserved slots
        update queue
        set reserved_slots = current_reserved_slots + expected_insertion_size
        where id = queue_id;
    end if;

    return dataset;
end;
$$ language plpgsql;

-- Function to release slot reservations after data insertion completes
-- Called after COPY operations finish to update the reserved_slots counter
-- This ensures accurate capacity tracking for dataset rotation decisions
create or replace function release_reservation(queue_id int, dataset_id int, actual_inserted int) returns void as
$$
begin
    -- Decrement reserved_slots by the actual number of rows inserted
    -- Use greatest(0, ...) to prevent negative values in case of race conditions
    -- Only update if current_dataset matches to handle concurrent dataset rotation
    update queue
    set reserved_slots = greatest(0, reserved_slots - actual_inserted)
    where id = queue_id
      and current_dataset = dataset_id;
end;
$$ language plpgsql;

-- Testing: Create a queue for initial setup
select *
from create_queue();