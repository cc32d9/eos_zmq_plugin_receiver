CREATE DATABASE eosio;

CREATE USER 'eosio'@'localhost' IDENTIFIED BY 'guugh3Ei';
GRANT ALL ON eosio.* TO 'eosio'@'localhost';
grant SELECT on eosio.* to 'eosioro'@'%' identified by 'eosioro';

use eosio;

CREATE TABLE EOSIO_VARS
 (
 varname VARCHAR(32) PRIMARY KEY,
 val_int BIGINT NULL,
 val_str VARCHAR(64) NULL
 ) ENGINE=InnoDB;

INSERT INTO EOSIO_VARS (varname, val_int) VALUES('last_irreversible_block', 0);

CREATE TABLE EOSIO_ACTIONS
 (
 id                BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
 global_action_seq BIGINT UNSIGNED NOT NULL,
 block_num         BIGINT NOT NULL,
 block_time        DATETIME NOT NULL,
 actor_account     VARCHAR(13) NOT NULL,
 recipient_account VARCHAR(13) NOT NULL,
 action_name       VARCHAR(13) NOT NULL,
 trx_id            VARCHAR(64) NOT NULL,
 jsdata            MEDIUMBLOB NOT NULL,
 irreversible      TINYINT NOT NULL DEFAULT 0,
 dup_seq           TINYINT NULL DEFAULT 0,
 status            TINYINT NULL DEFAULT 0
 ) ENGINE=InnoDB;

CREATE INDEX EOSIO_ACTIONS_I01 ON EOSIO_ACTIONS (global_action_seq);
CREATE INDEX EOSIO_ACTIONS_I03 ON EOSIO_ACTIONS (block_time);
CREATE INDEX EOSIO_ACTIONS_I04 ON EOSIO_ACTIONS (actor_account, action_name, block_num);
CREATE INDEX EOSIO_ACTIONS_I05 ON EOSIO_ACTIONS (recipient_account, action_name, block_num);
CREATE INDEX EOSIO_ACTIONS_I06 ON EOSIO_ACTIONS (trx_id(8));
CREATE INDEX EOSIO_ACTIONS_I07 ON EOSIO_ACTIONS (action_name, block_num);
CREATE INDEX EOSIO_ACTIONS_I08 ON EOSIO_ACTIONS (actor_account, block_num);
CREATE INDEX EOSIO_ACTIONS_I09 ON EOSIO_ACTIONS (irreversible, block_num);
CREATE INDEX EOSIO_ACTIONS_I10 ON EOSIO_ACTIONS (dup_seq, global_action_seq);
CREATE INDEX EOSIO_ACTIONS_I11 ON EOSIO_ACTIONS (status, global_action_seq);
CREATE INDEX EOSIO_ACTIONS_I12 ON EOSIO_ACTIONS (block_num, trx_id(2));




CREATE TABLE EOSIO_RESOURCE_BALANCES
 (
 action_id     BIGINT UNSIGNED NOT NULL,
 account_name  VARCHAR(13) NOT NULL,
 cpu_weight    DOUBLE PRECISION NOT NULL,
 cpu_used      INTEGER NOT NULL DEFAULT 0,
 cpu_available INTEGER NOT NULL DEFAULT 0,
 cpu_max       INTEGER NOT NULL DEFAULT 0,
 net_weight    DOUBLE PRECISION NOT NULL,
 net_used      INTEGER NOT NULL DEFAULT 0,
 net_available INTEGER NOT NULL DEFAULT 0,
 net_max       INTEGER NOT NULL DEFAULT 0,
 ram_quota     INTEGER NOT NULL,
 ram_usage     INTEGER NOT NULL,
 FOREIGN KEY (action_id)
   REFERENCES EOSIO_ACTIONS(id)
   ON DELETE CASCADE
) ENGINE=InnoDB;


CREATE INDEX EOSIO_RESOURCE_BALANCES_I01 ON EOSIO_RESOURCE_BALANCES (account_name, action_id);



CREATE TABLE EOSIO_CURRENCY_BALANCES
 (
 action_id     BIGINT UNSIGNED NOT NULL,
 account_name  VARCHAR(13) NOT NULL,
 issuer        VARCHAR(13) NOT NULL,
 currency      VARCHAR(8) NOT NULL,
 amount        DOUBLE PRECISION NOT NULL,
 FOREIGN KEY (action_id)
   REFERENCES EOSIO_ACTIONS(id)
   ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE INDEX EOSIO_CURRENCY_BALANCES_I01 ON EOSIO_CURRENCY_BALANCES
(account_name, issuer, currency, action_id);
CREATE INDEX EOSIO_CURRENCY_BALANCES_I02 ON EOSIO_CURRENCY_BALANCES (issuer, currency, action_id);



CREATE TABLE EOSIO_LATEST_RESOURCE
(
 account_name      VARCHAR(13) PRIMARY KEY,
 global_action_seq BIGINT UNSIGNED NOT NULL,
 cpu_weight        DOUBLE PRECISION NOT NULL,
 cpu_used          INTEGER NOT NULL DEFAULT 0,
 cpu_available     INTEGER NOT NULL DEFAULT 0,
 cpu_max           INTEGER NOT NULL DEFAULT 0,
 net_weight        DOUBLE PRECISION NOT NULL,
 net_used          INTEGER NOT NULL DEFAULT 0,
 net_available     INTEGER NOT NULL DEFAULT 0,
 net_max           INTEGER NOT NULL DEFAULT 0,
 ram_quota         INTEGER NOT NULL,
 ram_usage         INTEGER NOT NULL
) ENGINE=InnoDB;


CREATE TABLE EOSIO_LATEST_CURRENCY
 (
 account_name      VARCHAR(13) NOT NULL,
 global_action_seq BIGINT UNSIGNED NOT NULL,
 issuer            VARCHAR(13) NOT NULL,
 currency          VARCHAR(8) NOT NULL,
 amount            DOUBLE PRECISION NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX EOSIO_LATEST_CURRENCY_I01 ON EOSIO_LATEST_CURRENCY (account_name, issuer, currency);
CREATE INDEX EOSIO_LATEST_CURRENCY_I02 ON EOSIO_LATEST_CURRENCY (issuer, currency, amount);
CREATE INDEX EOSIO_LATEST_CURRENCY_I03 ON EOSIO_LATEST_CURRENCY (currency, issuer);



/* transfers may be inline actions, so global_seq may have values
   not found in EOSIO_ACTIONS table */

CREATE TABLE EOSIO_TRANSFERS
(
 global_seq  BIGINT UNSIGNED PRIMARY KEY,
 block_num   BIGINT NOT NULL,
 block_time  DATETIME NOT NULL,
 trx_id      VARCHAR(64) NOT NULL,
 issuer      VARCHAR(13) NOT NULL,
 currency    VARCHAR(8) NOT NULL,
 amount      DOUBLE PRECISION NOT NULL,
 tx_from     VARCHAR(13) NULL,
 tx_to       VARCHAR(13) NOT NULL,
 memo        TEXT,
 bal_from    DOUBLE PRECISION NULL,
 bal_to      DOUBLE PRECISION NOT NULL
)  ENGINE=InnoDB;


CREATE INDEX EOSIO_TRANSFERS_I01 ON EOSIO_TRANSFERS (block_num);
CREATE INDEX EOSIO_TRANSFERS_I02 ON EOSIO_TRANSFERS (block_time);
CREATE INDEX EOSIO_TRANSFERS_I03 ON EOSIO_TRANSFERS (trx_id(8));
CREATE INDEX EOSIO_TRANSFERS_I04 ON EOSIO_TRANSFERS (tx_from, tx_to, issuer, currency, block_num);
CREATE INDEX EOSIO_TRANSFERS_I05 ON EOSIO_TRANSFERS (tx_to, tx_from, issuer, currency, block_num);
CREATE INDEX EOSIO_TRANSFERS_I06 ON EOSIO_TRANSFERS (tx_from, issuer, currency, block_num);
CREATE INDEX EOSIO_TRANSFERS_I07 ON EOSIO_TRANSFERS (tx_to, issuer, currency, block_num);
CREATE INDEX EOSIO_TRANSFERS_I08 ON EOSIO_TRANSFERS (tx_to, block_num);
CREATE INDEX EOSIO_TRANSFERS_I09 ON EOSIO_TRANSFERS (tx_from, block_num);
CREATE INDEX EOSIO_TRANSFERS_I10 ON EOSIO_TRANSFERS (tx_to, global_seq);
CREATE INDEX EOSIO_TRANSFERS_I11 ON EOSIO_TRANSFERS (tx_from, global_seq);
