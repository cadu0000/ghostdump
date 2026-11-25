--
-- PostgreSQL database dump
--
SET standard_conforming_strings = on;

COPY public.users (id, name, email, salary, note) FROM stdin;
1	Ana Souza	ana@example.com	5400.50	tem (parenteses)
2	O'Hara	\N	3100.00	C:\\temp
3	Bruno	bruno@example.com	\N	

\.

INSERT INTO public.users (id, name, email, salary, note) VALUES (4, 'Carla', 'carla@example.com', 7200.25, NULL);
INSERT INTO public.users (id, name, email, salary, note) VALUES (5, 'D''Ávila', NULL, 2950.00, 'a, b') ON CONFLICT DO NOTHING;

SELECT pg_catalog.setval('public.users_id_seq', 5, true);
