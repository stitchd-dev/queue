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
create table destination
(
    id     serial primary key,
    name   varchar(255) not null,
    type   varchar(255) not null,
    config jsonb
);

create table source
(
    id   serial primary key,
    name varchar(255) not null,
    type varchar(255) not null
);

create table queue
(
    id                  serial primary key,
    destination_id      int references destination (id) not null,
    created_at          timestamp                       not null default now(),
    updated_at          timestamp                       not null default now(),
    reserved_slots      int                             not null default 0,
    current_dataset     int                             not null default 0
        constraint queue_current_dataset_check check (current_dataset >= 0),
    processing_dataset  int                             not null default 0
        constraint queue_processing_dataset_check check ( processing_dataset >= 0 and processing_dataset <= current_dataset ),
    last_failed_dataset int                             not null default 0
        constraint queue_last_failed_dataset_check check ( last_failed_dataset >= 0 and last_failed_dataset <= processing_dataset )
);

create index queue_destination_id_idx on queue (destination_id);

create type job_status as enum ('pending', 'processing', 'failed', 'done', 'failed_permanently');

create or replace function create_dataset(queue_id int, dataset_id int) returns void as
$$
declare
    data_table_name   text;
    job_table_name    text;
    status_index_name text;
    try_at_index_name text;
begin
    data_table_name := format('queue_%s_data_%s', queue_id, dataset_id);
    job_table_name := format('queue_%s_job_%s', queue_id, dataset_id);
    status_index_name := format('queue_%s_job_%s_status_idx', queue_id, dataset_id);
    try_at_index_name := format('queue_%s_job_%s_try_at_idx', queue_id, dataset_id);


    execute format(
            'create table %I (
                id     uuid primary key,
                data   jsonb not null,
                source int references source (id) not null
            )',
            data_table_name);
    execute format(
            'create table %I (
                id         serial primary key,
                status     job_status not null default ''pending'',
                data       uuid not null,
                try_at     timestamp not null default now(),
                retry_count int not null default 0,
                created_at timestamp not null default now(),
                updated_at timestamp not null default now()
            )',
            job_table_name, data_table_name);


    execute format('create index %I on %I (status)', status_index_name, job_table_name);
    execute format('create index %I on %I (try_at)', try_at_index_name, job_table_name);
end;
$$ language plpgsql;

create or replace function add_destination(name varchar(255), type varchar(255), config jsonb) returns int as
$$
declare
    destination_id int;
    queue_id       int;
    dataset_id     int;
begin
    insert into destination (name, type, config) values (name, type, config) returning id into destination_id;
    insert into queue (destination_id) values (destination_id) returning id, current_dataset into queue_id, dataset_id;

    perform create_dataset(queue_id, dataset_id);

    return destination_id;
end;
$$ language plpgsql;

create type failed_job_update as
(
    id          int,
    try_at      timestamp,
    retry_count int
);

create or replace function update_status(queue_id int, dataset_id int, done_jobs int[],
                                         failed_permanently_jobs int[], failed_jobs failed_job_update[]) returns void as
$$
declare
    table_name text;
begin
    table_name := format('queue_%s_job_%s', queue_id, dataset_id);

    if array_length(done_jobs, 1) > 0 then
        execute format('update %I set
                           status = ''done'',
                           updated_at = now()
                       where id = any($1)',
                       table_name) using done_jobs;
    end if;

    if array_length(failed_permanently_jobs, 1) > 0 then
        execute format('update %I set
                            status = ''failed_permanently'',
                            updated_at = now()
                       where id = any($1)',
                       table_name) using failed_permanently_jobs;
    end if;

    if array_length(failed_jobs, 1) > 0 then
        execute format(
                'update %I set
                    status = ''failed'',
                    updated_at = now(),
                    try_at = f.try_at,
                    retry_count = f.retry_count
                from unnest($1) as f where %I.id = f.id',
                table_name) using failed_jobs;
    end if;
end;
$$ language plpgsql;

create or replace function get_current_dataset(queue_id int, threshold int, expected_insertion_size int) returns int as
$$
declare
    current_dataset int;
    reserved_slots  int;
    job_table_name  text;
    count           int;
begin
    select queue.current_dataset, queue.reserved_slots
    into current_dataset, reserved_slots
    from queue
    where id = queue_id for update;

    job_table_name := format('queue_%s_job_%s', queue_id, current_dataset);
    execute format('select count(*) from %I', job_table_name) into count;

    if count >= threshold - expected_insertion_size - reserved_slots then
        current_dataset := current_dataset + 1;
        update queue
        set current_dataset = current_dataset,
            reserved_slots  = expected_insertion_size
        where id = queue_id;
        perform create_dataset(queue_id, current_dataset);
    else
        update queue
        set reserved_slots = reserved_slots + expected_insertion_size
        where id = queue_id;
    end if;
    return current_dataset;
end;
$$ language plpgsql;

create or replace function release_reservation(queue_id int, dataset_id int, actual_inserted int) returns void as
$$
begin
    update queue
    set reserved_slots = greatest(0, reserved_slots - actual_inserted)
    where id = queue_id
      and current_dataset = dataset_id;
end;
$$ language plpgsql;