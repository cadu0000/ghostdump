--
-- PostgreSQL database dump
--

\restrict bv7OWmhGsRXVxtd5Jspef71P6wWKDF4Mkz9nq8uVUyPTM8T26phiXwe6TX2tdFo

-- Dumped from database version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: users; Type: TABLE; Schema: public; Owner: root
--

CREATE TABLE public.users (
    id integer,
    name text,
    photo oid
);


ALTER TABLE public.users OWNER TO root;

--
-- Name: 49158; Type: BLOB; Schema: -; Owner: root
--

SELECT pg_catalog.lo_create('49158');


ALTER LARGE OBJECT 49158 OWNER TO root;

--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: root
--

COPY public.users (id, name, photo) FROM stdin;
1	Ana	49158
\.


--
-- Data for Name: BLOBS; Type: BLOBS; Schema: -; Owner: -
--

BEGIN;

SELECT pg_catalog.lo_open('49158', 131072);
SELECT pg_catalog.lowrite(0, '\x534543524554204c4f204259544553');
SELECT pg_catalog.lo_close(0);

COMMIT;

--
-- PostgreSQL database dump complete
--

\unrestrict bv7OWmhGsRXVxtd5Jspef71P6wWKDF4Mkz9nq8uVUyPTM8T26phiXwe6TX2tdFo

