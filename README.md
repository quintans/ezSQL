# ezSQL
Simple ORM library for JAVA

(This document is a work in progress)

## Index

* [Introduction](#introduction)
* [SimpleJDBC](#simplejdbc)
* [Environment](#environment)

## Introduction

**ezSQL** is a framework that aims to ease the way a user executes SQL against a database.

It provides an easy way of executing **static typed** SQL and to project results to arbitrary Objects. To handle SQL, without hiding it, is the main goal of the framework. ORM features are just a bonus.

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
ezSQL is build on top of utility class named SimpleJDBC. This class isolates the developer from the details of JDBC call, like statement creation, exception handling, parameters handling, etc
This class does not depend of ezSQL. The developer can use this class if it needs a low level control.


## Environment
To use the framework we must need to know the database structure. It is not a static configuration, and we could use it in a dynamic schema (altered in runtime by adding tables, columns) with the proper supporting code. The common use is to define the database structure as exemplified in the following sections.
