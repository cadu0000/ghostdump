--
-- PostgreSQL database dump
--

\restrict v0nZaJbhFB8al18Zeax6dOglrQN6VkmTs95aKyr2YhuLlYbIWgGEcGLSVtsuU8u

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
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: add_user(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.add_user(n text) RETURNS void
    LANGUAGE sql
    BEGIN ATOMIC
 INSERT INTO public.users (name, email)
   VALUES (add_user.n, 'new@example.com'::text);
 UPDATE public.users SET email =
         CASE
             WHEN (users.email IS NULL) THEN 'none@example.com'::text
             ELSE users.email
         END
   WHERE (users.name = add_user.n);
END;


ALTER FUNCTION public.add_user(n text) OWNER TO postgres;

--
-- Name: log; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.log (
    x text
);


ALTER TABLE public.log OWNER TO postgres;

--
-- Name: my table; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."my table" (
    "first name" text
);


ALTER TABLE public."my table" OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Data for Name: log; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.log (x) FROM stdin;
Carol Example
second action
\.


--
-- Data for Name: my table; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public."my table" ("first name") FROM stdin;
Carol Example
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.users (id, name, email) FROM stdin;
1	Alice Example	alice@example.com
2	Bob O'Brien	\N
\.


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.users_id_seq', 2, true);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: my table keep_log; Type: RULE; Schema: public; Owner: postgres
--

CREATE RULE keep_log AS
    ON INSERT TO public."my table" DO ( INSERT INTO public.log (x)
  VALUES (new."first name");
 INSERT INTO public.log (x)
  VALUES ('second action'::text);
);


--
-- PostgreSQL database dump complete
--

\unrestrict v0nZaJbhFB8al18Zeax6dOglrQN6VkmTs95aKyr2YhuLlYbIWgGEcGLSVtsuU8u

