--liquibase formatted sql

--changeset maxdrv:create_pg_enum_status
CREATE TYPE sortable_status AS ENUM ('KEEPED_DIRECT', 'SORTED_DIRECT');

--changeset maxdrv:add_pg_enum_append
ALTER TYPE sortable_status ADD VALUE 'SHIPPED_DIRECT';

--changeset maxdrv:add_pg_enum_before
ALTER TYPE sortable_status ADD VALUE 'ARRIVED_DIRECT' BEFORE 'KEEPED_DIRECT';

--changeset maxdrv:add_pg_enum_after
ALTER TYPE sortable_status ADD VALUE 'PREPARED_DIRECT' AFTER 'SORTED_DIRECT';


--changeset maxdrv:create_pg_enum_type
CREATE TYPE sortable_type AS ENUM ('PLACE', 'PALLET');


--changeset maxdrv:create_table_with_enum
CREATE TABLE IF NOT EXISTS sortable
(
    id     BIGSERIAL PRIMARY KEY,
    status sortable_status NOT NULL,
    type   sortable_type
);
