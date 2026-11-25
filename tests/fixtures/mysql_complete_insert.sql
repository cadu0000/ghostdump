-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)
--
-- Host: localhost    Database: shop
-- ------------------------------------------------------
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;

DROP TABLE IF EXISTS `customers`;
CREATE TABLE `customers` (
  `id` int NOT NULL,
  `name` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `customers` WRITE;
INSERT INTO `customers` (`id`, `name`, `email`) VALUES (1,'Ana O\'Neil','ana@example.com'),(2,'Bruno','bruno@example.com');
UNLOCK TABLES;
