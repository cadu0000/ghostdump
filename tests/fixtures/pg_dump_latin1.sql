--
-- PostgreSQL database dump
--

\restrict iKgAnlH59ochaEsdwDpqVI5beo58Kam9HE3Ik5hcxgKo5g34dgIpcxaaLQSZiic

-- Dumped from database version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'LATIN1';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: people; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.people (
    id integer NOT NULL,
    name text,
    city text
);


ALTER TABLE public.people OWNER TO postgres;

--
-- Data for Name: people; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.people (id, name, city) FROM stdin;
1	José	São Paulo
2	Josè	Brasília
3	Josë	Belém
4	Ana	Natal
\.


--
-- Name: people people_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.people
    ADD CONSTRAINT people_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict iKgAnlH59ochaEsdwDpqVI5beo58Kam9HE3Ik5hcxgKo5g34dgIpcxaaLQSZiic

