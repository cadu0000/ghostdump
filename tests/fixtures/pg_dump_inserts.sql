--
-- PostgreSQL database dump
--

\restrict GmRiIq3QinujD2TnxzO2jeCTCnKwdOB9mypXrBlITtvWPyXswlis89SCaY9Ge88

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
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text
);


--
-- Name: add_user(text); Type: FUNCTION; Schema: public; Owner: -
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


--
-- Name: log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.log (
    x text
);


--
-- Name: my table; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."my table" (
    "first name" text
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
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
-- Data for Name: log; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.log VALUES ('Carol Example');
INSERT INTO public.log VALUES ('second action');


--
-- Data for Name: my table; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."my table" VALUES ('Carol Example');


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.users OVERRIDING SYSTEM VALUE VALUES (1, 'Alice Example', 'alice@example.com');
INSERT INTO public.users OVERRIDING SYSTEM VALUE VALUES (2, 'Bob O''Brien', NULL);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.users_id_seq', 2, true);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: my table keep_log; Type: RULE; Schema: public; Owner: -
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

\unrestrict GmRiIq3QinujD2TnxzO2jeCTCnKwdOB9mypXrBlITtvWPyXswlis89SCaY9Ge88

