# The esoco-storage project

[![](https://github.com/esoco/esoco-storage/workflows/Build/badge.svg)](https://github.com/esoco/esoco-storage/actions)

This project contains the esoco-storage framework which offers a simple but powerful API to implement object persistence. The generic API not only supports JDBC-based relational persistence engines but can also easily be adapted to other persistence concepts or back-ends. It provides direct persistence for arbitrary Java objects (POJOs) without the need for configuration other than defining the actual database connection. It is built on the ObjectRelations framework and the functional programming framework therein. Predicates from the latter are used for the definition of query criteria in a fluent way.

 For detailed information please see the [Javadoc](http://esoco.github.io/esoco-storage/javadoc/).

# License

This project is licensed under the Apache 2.0 license (see LICENSE file for details).  