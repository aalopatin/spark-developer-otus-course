-- Table: public.distance_division

-- DROP TABLE IF EXISTS public.distance_division;

CREATE TABLE IF NOT EXISTS public.distance_division
(
    "from" integer NOT NULL,
    "to" integer NOT NULL,
    count bigint NOT NULL,
    average double precision,
    deviation double precision,
    min double precision,
    max double precision
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.distance_division
    OWNER to docker;