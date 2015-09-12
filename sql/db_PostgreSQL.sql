CREATE USER quintans WITH PASSWORD 'quintans';
CREATE DATABASE ezsql;
GRANT ALL PRIVILEGES ON DATABASE ezsql to quintans;

drop table GALLERY_PAINTING;
drop table PAINTING;
DROP TABLE ARTIST;
drop table IMAGE;
drop table GALLERY;

drop table BOOK_I18N;
drop table BOOK;
drop table AUTHOR;

DROP table employee;

DROP TABLE TB;
DROP TABLE TC;
DROP TABLE TA;

DROP TABLE CATALOG;
DROP TABLE TEMPORAL;

CREATE TABLE ARTIST(
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
   	NAME VARCHAR(255) NOT NULL,
	GENDER VARCHAR(1) NOT NULL,
	BIRTHDAY TIMESTAMP,
	CREATION TIMESTAMP,
	MODIFICATION TIMESTAMP
);
ALTER SEQUENCE ARTIST_id_seq RESTART WITH 10;

CREATE TABLE PAINTING(
    ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
    NAME VARCHAR(255) NOT NULL,
    PRICE DECIMAL(20, 2),
    ARTIST_ID INTEGER,
    IMAGE_ID INTEGER
);
ALTER SEQUENCE PAINTING_id_seq RESTART WITH 10;

CREATE TABLE IMAGE(
    ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
    CONTENT bytea
);
ALTER SEQUENCE IMAGE_id_seq RESTART WITH 10;

CREATE TABLE GALLERY (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
	NAME VARCHAR(255) NOT NULL,
	ADDRESS VARCHAR(255)
);
ALTER SEQUENCE GALLERY_id_seq RESTART WITH 10;

CREATE TABLE GALLERY_PAINTING (
	GALLERY INTEGER NOT NULL,
	PAINTING INTEGER NOT NULL
);

ALTER TABLE GALLERY_PAINTING ADD CONSTRAINT GALERY_PAINTING_PK PRIMARY KEY (GALLERY,PAINTING);
ALTER TABLE GALLERY_PAINTING ADD CONSTRAINT GALERY_PAINTING_FK1 FOREIGN KEY (GALLERY) REFERENCES GALLERY(ID);
ALTER TABLE GALLERY_PAINTING ADD CONSTRAINT GALERY_PAINTING_FK2 FOREIGN KEY (PAINTING) REFERENCES PAINTING(ID);

ALTER TABLE PAINTING ADD CONSTRAINT PAINTING_FK1 FOREIGN KEY(ARTIST_ID) REFERENCES ARTIST(ID);
ALTER TABLE PAINTING ADD CONSTRAINT PAINTING_FK2 FOREIGN KEY(IMAGE_ID) REFERENCES IMAGE(ID);


----------
-- i18n --
----------
CREATE TABLE AUTHOR(
    ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
    NAME VARCHAR(255) NOT NULL,
	BIRTHDAY TIMESTAMP
);
ALTER SEQUENCE AUTHOR_id_seq RESTART WITH 10;

CREATE TABLE BOOK(
    ID BIGSERIAL,
	PRIMARY KEY(ID),
	VERSION INTEGER NOT NULL,
	PRICE DECIMAL,
    AUTHOR_ID INTEGER
);

ALTER TABLE BOOK ADD CONSTRAINT FK1_BOOK FOREIGN KEY(AUTHOR_ID) REFERENCES AUTHOR(ID);

CREATE TABLE BOOK_I18N(
    ID INTEGER NOT NULL,
	LANG VARCHAR(10) NOT NULL,
    NAME VARCHAR(255) NOT NULL
);

ALTER TABLE BOOK_I18N ADD PRIMARY KEY (ID, LANG);
ALTER TABLE BOOK_I18N ADD CONSTRAINT FK1_BOOK_I18N FOREIGN KEY(ID) REFERENCES BOOK(ID);



----
-- performance tests
----

create table employee (
    id INTEGER,
	PRIMARY KEY(ID),
    name varchar(100) not null,
	sex boolean,
	PAY_GRADE INTEGER,
    CREATION timestamp
);

--

CREATE TABLE TA (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	TIPO VARCHAR(255),
	FK INTEGER
);
ALTER SEQUENCE TA_id_seq RESTART WITH 10;

CREATE TABLE TB (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	DSC VARCHAR(255),
	FK INTEGER
);
ALTER SEQUENCE TB_id_seq RESTART WITH 10;

CREATE TABLE TC (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	DSC VARCHAR(255),
	FK INTEGER
);
ALTER SEQUENCE TC_id_seq RESTART WITH 10;

--

CREATE TABLE CATALOG (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
	KIND VARCHAR(30),
	TOKEN VARCHAR(30),
	VALUE VARCHAR(255)
);
ALTER SEQUENCE CATALOG_id_seq RESTART WITH 10;

CREATE TABLE TEMPORAL (
	ID BIGSERIAL,
	PRIMARY KEY(ID),
    CLOCK TIME NULL,
	CALENDAR DATE NULL,
	NOW TIMESTAMPTZ NULL,
	INSTANT TIMESTAMP NULL
);
ALTER SEQUENCE TEMPORAL_id_seq RESTART WITH 10;
