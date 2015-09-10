# ezSQL
Simple ORM library for JAVA

(This document is a work in progress)

## Index

* [Introduction](#introduction)
* [SimpleJDBC](#simplejdbc)
* [Environment](#environment)

## Introduction

**ezSQL** is a framework that aims to ease the way a user executes SQL against a database.

It provides an easy way of executing **static typed** SQL and to project results to arbitrary Objects.
To handle SQL, without hiding it, is the main goal of the framework. ORM features are just a bonus.

This project is the same as goSQL, but in java.

Main Features:
* SQL DSL
* Simple declaration of joins
* Static typed
* Database abstraction
* Result transformation
* Result Pagination
* Simple Object Relationship Mapping
* Optimistic locking
* Extensible

## SimpleJDBC
ezSQL is build on top of utility class named SimpleJDBC.
This class isolates the developer from the details of JDBC call, like statement creation,
exception handling, parameters handling, etc
This class does not depend of ezSQL. The developer can use this class if it needs a low level control.


## Environment
To use the framework we must need to know the database structure.
It is not a static configuration, and we could use it in a dynamic schema (altered in runtime by adding tables, columns)
with the proper supporting code. The common use is to define the database structure as exemplified in the following sections.

### Entity Relation Diagram
The examples in the next chapter will use the tables that are defined according to the following ER diagram:

![ER Diagram](test/er.png)

Relationships explained:
- **One-to-Many**: One `ARTIST` that can have many `PAINTING`'s and one `PAINTING` has one `ARTIST`.
- **One-to-One**: One `PAINTING` has one `IMAGE` and one `IMAGE` has one `PAINTING`.
- **Many-to-Many**: One `PAINTING` can have many `GALLERY`'s and one `GALLERY` can have many `PAINTING`'s

Despite the previous explanation of relationships, they are declared in the same way.
Think of them as paths to get from one table to another.

### Table ARTIST definition

Definition of a table ARTIST and its columns, identifying key fields, version field, etc

```java
import java.util.Date;

import pt.quintans.ezSQL.db.*;
import pt.quintans.ezSQL.orm.app.domain.EGender;

public class TArtist {
    public static final Table T_ARTIST = new Table("ARTIST");

    public static final Column<Long> C_ID = T_ARTIST.BIGINT("ID").key();
    public static final Column<Integer> C_VERSION = T_ARTIST.INTEGER("VERSION").version();
    public static final Column<String> C_NAME = T_ARTIST.VARCHAR("NAME");
    public static final Column<EGender> C_GENDER = T_ARTIST.NAMED("GENDER");
    public static final Column<Date> C_BIRTHDAY = T_ARTIST.TIMESTAMP("BIRTHDAY");

    // ONE artist has MANY paintings
    public final static Association A_PAINTINGS = T_ARTIST
            .ASSOCIATE(C_ID).TO(TPainting.C_ARTIST).AS("paintings");
}

```

For the table and columns a default alias is created, if none is supplied, based in the name.
For example, an alias “firstName” will be created for a column or table named “FIRST_NAME”.
Usually we can omit the alias definition.

Notice that the declaration of the relationship does not indicate if it is many-to-one, one-to-one, etc.
Many-to-many declaration is almost the same as seen next.
