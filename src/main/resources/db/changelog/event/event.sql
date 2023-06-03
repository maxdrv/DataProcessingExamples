--liquibase formatted sql

--changeset maxdrv:create_event_table
create table if not exists event
(
    id                bigserial      primary key,
    created_at        timestamptz not null,
    updated_at        timestamptz not null,
    type              text not null,
    payload           text
);