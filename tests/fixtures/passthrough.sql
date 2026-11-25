--
-- PostgreSQL database dump
--
\restrict 8Kf2pQ'x
SET standard_conforming_strings = on;
/* INSERT INTO fake VALUES (1); COPY fake FROM stdin; */
CREATE TABLE public.events (
    id integer NOT NULL,
    inserted_at timestamp,
    copy_count integer DEFAULT 0
);

CREATE FUNCTION public.audit() RETURNS trigger AS $body$
BEGIN
    INSERT INTO public.log VALUES (NEW.id);
    RETURN NEW;
END;
$body$ LANGUAGE plpgsql;

SELECT 'C:\' AS path;
SELECT E'it\'s INSERT INTO x VALUES (1)';
INSERT INTO public.archive SELECT * FROM public.events;
COPY public.events TO stdout;
-- trailing comment with a quote '
SELECT 1;