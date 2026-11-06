--
-- PostgreSQL database dump
--
SET standard_conforming_strings = on;

COPY public.users (id, name, email, salary, note) FROM stdin;
1	Damian Graham	78d72a7454a58eb7833e4c30b19cf2681f1f5f6e97eb038de7d24806f50d470d	14307.13	QNVy7rDF
2	Brianne Gaylord	\N	15875.56	bnTwybfF
3	Zoe Rodriguez	19f4609285b0d07c61dec7c841627051e42d803dfacfcc0c9e84832a1a99d9c8	\N	3kr3IaW3

\.

INSERT INTO public.users (id, name, email, salary, note) VALUES (4, 'Maiya Gislason', 'aa8630867232dd760eef1ea600b5390a5870ba1dd60cfa6a140340096fd53520', -12736.42, 'aZSWzwqL');
INSERT INTO public.users (id, name, email, salary, note) VALUES (5, 'Ryan Rogahn', NULL, 24840.08, 'k2cI3cVb') ON CONFLICT DO NOTHING;

SELECT pg_catalog.setval('public.users_id_seq', 5, true);
