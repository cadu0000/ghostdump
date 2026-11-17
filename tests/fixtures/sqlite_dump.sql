PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
INSERT INTO users VALUES(1,'Alice Example','alice@example.com');
INSERT INTO users VALUES(2,'Bob O''Brien',NULL);
CREATE TABLE audit (id INTEGER PRIMARY KEY, user_name TEXT);
CREATE TRIGGER users_audit AFTER UPDATE ON users BEGIN UPDATE users SET email = lower(email) WHERE id = NEW.id; INSERT INTO audit (user_name) VALUES (NEW.name); END;
COMMIT;
