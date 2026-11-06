--
-- PostgreSQL database dump
--
SET standard_conforming_strings = on;

COPY public.users (id, name, email, salary, note) FROM stdin;
1	Bret Leffler	78d72a7454a58eb7833e4c30b19cf2681f1f5f6e97eb038de7d24806f50d470d	4087.72	NoAKhdDq
2	Devon Sporer	\N	37117.13	kmFLZtt9
3	Torrance Gaylord	19f4609285b0d07c61dec7c841627051e42d803dfacfcc0c9e84832a1a99d9c8	\N	FJ2Okqy0

\.

INSERT INTO public.users (id, name, email, salary, note) VALUES (4, 'Norris D''Amore', 'aa8630867232dd760eef1ea600b5390a5870ba1dd60cfa6a140340096fd53520', 4734.50, 'UewxviM5');
INSERT INTO public.users (id, name, email, salary, note) VALUES (5, 'Akeem Carroll', NULL, 34678.41, 'AjqJIghx') ON CONFLICT DO NOTHING;

SELECT pg_catalog.setval('public.users_id_seq', 5, true);
