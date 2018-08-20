CREATE DATABASE eosio;

CREATE USER 'eosio'@'localhost' IDENTIFIED BY 'guugh3Ei';
GRANT ALL ON eosio.* TO 'eosio'@'localhost';

use eosio;

CREATE TABLE EOSIO_ACTIONS
 (
 global_action_seq BIGINT PRIMARY KEY,
 block_num BIGINT NOT NULL,
 block_time DATETIME NOT NULL,
 actor_account VARCHAR(13) NOT NULL,
 recipient_account VARCHAR(13) NOT NULL,
 action_name VARCHAR(13) NOT NULL,
 trx_id VARCHAR(64) NOT NULL,
 jsdata BLOB NOT NULL
 ) ENGINE=InnoDB;

CREATE INDEX EOSIO_ACTIONS_I01 ON EOSIO_ACTIONS (block_num);
CREATE INDEX EOSIO_ACTIONS_I02 ON EOSIO_ACTIONS (block_time);
CREATE INDEX EOSIO_ACTIONS_I03 ON EOSIO_ACTIONS (actor_account, action_name, block_num);
CREATE INDEX EOSIO_ACTIONS_I04 ON EOSIO_ACTIONS (recipient_account, action_name, block_num);
CREATE INDEX EOSIO_ACTIONS_I05 ON EOSIO_ACTIONS (trx_id(8));
CREATE INDEX EOSIO_ACTIONS_I06 ON EOSIO_ACTIONS (action_name, block_num);
 
CREATE TABLE EOSIO_RESOURCE_BALANCES
 (
 global_action_seq BIGINT NOT NULL,
 account_name VARCHAR(13) NOT NULL,
 cpu_weight DOUBLE PRECISION NOT NULL,
 cpu_used INTEGER NOT NULL DEFAULT 0,
 cpu_available INTEGER NOT NULL DEFAULT 0,
 cpu_max INTEGER NOT NULL DEFAULT 0,
 net_weight DOUBLE PRECISION NOT NULL,
 net_used INTEGER NOT NULL DEFAULT 0,
 net_available INTEGER NOT NULL DEFAULT 0,
 net_max INTEGER NOT NULL DEFAULT 0,
 ram_quota INTEGER NOT NULL,
 ram_usage INTEGER NOT NULL,
 FOREIGN KEY (global_action_seq)
   REFERENCES EOSIO_ACTIONS(global_action_seq)
   ON DELETE CASCADE
) ENGINE=InnoDB;


CREATE INDEX EOSIO_RESOURCE_BALANCES_I01 ON EOSIO_RESOURCE_BALANCES (global_action_seq);
CREATE INDEX EOSIO_RESOURCE_BALANCES_I02 ON EOSIO_RESOURCE_BALANCES (account_name, global_action_seq);



CREATE TABLE EOSIO_CURRENCY_BALANCES
 (
 global_action_seq BIGINT NOT NULL,
 account_name VARCHAR(13) NOT NULL,
 issuer VARCHAR(13) NOT NULL,
 currency VARCHAR(8) NOT NULL,
 amount DOUBLE PRECISION NOT NULL,
 FOREIGN KEY (global_action_seq)
   REFERENCES EOSIO_ACTIONS(global_action_seq)
   ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE INDEX EOSIO_CURRENCY_BALANCES_I01 ON EOSIO_CURRENCY_BALANCES (global_action_seq);
CREATE INDEX EOSIO_CURRENCY_BALANCES_I02 ON EOSIO_CURRENCY_BALANCES
(account_name, issuer, currency, global_action_seq);
CREATE INDEX EOSIO_CURRENCY_BALANCES_I03 ON EOSIO_CURRENCY_BALANCES (issuer, currency, global_action_seq);



CREATE TABLE EOSIO_LATEST_RESOURCE
(
 account_name VARCHAR(13) PRIMARY KEY,
 global_action_seq BIGINT NOT NULL,
 cpu_weight DOUBLE PRECISION NOT NULL,
 cpu_used INTEGER NOT NULL DEFAULT 0,
 cpu_available INTEGER NOT NULL DEFAULT 0,
 cpu_max INTEGER NOT NULL DEFAULT 0,
 net_weight DOUBLE PRECISION NOT NULL,
 net_used INTEGER NOT NULL DEFAULT 0,
 net_available INTEGER NOT NULL DEFAULT 0,
 net_max INTEGER NOT NULL DEFAULT 0,
 ram_quota INTEGER NOT NULL,
 ram_usage INTEGER NOT NULL
) ENGINE=InnoDB;


CREATE TABLE EOSIO_LATEST_CURRENCY
 (
 account_name VARCHAR(13) NOT NULL,
 global_action_seq BIGINT NOT NULL,
 issuer VARCHAR(13) NOT NULL,
 currency VARCHAR(8) NOT NULL,
 amount DOUBLE PRECISION NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX EOSIO_LATEST_CURRENCY_I01 ON EOSIO_LATEST_CURRENCY (account_name, issuer, currency);
CREATE INDEX EOSIO_LATEST_CURRENCY_I02 ON EOSIO_LATEST_CURRENCY (issuer, currency, amount);


