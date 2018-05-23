# Muses: Distributed Data Migration System for Polystores 
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/bdapro-muses/) [![Build Status](https://travis-ci.com/mi-1-0-0/muses.svg?token=smj1XV9m8BFqsSyVXceY&branch=master)](https://github.com/mi-1-0-0/muses)


Muses is a distributed data migration system for heterogeneous data stores. It uses [Apache Arrow](https://arrow.apache.org/), which is an in-memory columnar datastore, as an intermediate data store. Other application will use the Apache Arrow to read the data. Data migration is done in streaming manners, instead of batching.

## Getting Started

To start the data migration, run the ```run-muses.sh``` script. It takes the following parameters: 

- source data store information


### Prerequisites

You need the following tools to run the program: 

- JDK 1.8 or above
- Maven
- Scala

In addition, Maven uses Apache Arrow, Netty, Akka, SLF4J, and Scala Compiler as dependencies. For more details, see the ```pom.xml``` files.

### Installing

Muses is a protable system, so no additional installation steps are required.

## Running the tests

To run the unit tests, ```jUnit 4.11``` is used. To run the tests and see the results, run the corresponding test jar files. 


## Deployment

You need the following tools to run and deploy the system:

- JDK 1.8 or above

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Contributing

Please read [CONTRIBUTING.md](https://github.com/mi-1-0-0/muses/blob/master/CONTRIBUTING.md) for details.


## License

This project is Licensed (by default) - see the [LICENSE.md](https://github.com/mi-1-0-0/muses/blob/master/LICENSE.md) file for details.
