--
-- PostgreSQL database dump
--
SET standard_conforming_strings = on;

COPY public.users (id, name, email, salary, note) FROM stdin;
1	Bret Leffler	78d72a7454a58eb7833e4c30b19cf2681f1f5f6e97eb038de7d24806f50d470d	13225.21	oZCnaWvL
2	Rosina Murphy	\N	-3427.81	07mg3ZtJ
3	Princess Lebsack	19f4609285b0d07c61dec7c841627051e42d803dfacfcc0c9e84832a1a99d9c8	\N	0NoAKhdD

\.

INSERT INTO public.users (id, name, email, salary, note) VALUES (4, 'Makayla Oberbrunner', 'aa8630867232dd760eef1ea600b5390a5870ba1dd60cfa6a140340096fd53520', -11494.18, 'fgaDFWTc');
INSERT INTO public.users (id, name, email, salary, note) VALUES (5, 'Brook Schimmel', NULL, 5225.72, 'ZKp3bM47') ON CONFLICT DO NOTHING;

SELECT pg_catalog.setval('public.users_id_seq', 5, true);
