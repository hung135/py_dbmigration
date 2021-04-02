# PY_DBMIGRATION

This is a python framework that will facility the loading of various data files into a postgres database.  The framework will do various things for the user:
```
Inventory raw files
Manage file state
Provides ability to write custom ingestion logic
Allows parrallel ingestion
Allows for multi-core ingestion
Writes to logging data table
Allows for reprocessing of failed filed
Yaml driven configuration
Pattern file matching via REGEX
```

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.



## Dev Environment

* [vscode] (https://code.visualstudio.com/) - Development IDE
    ```brew cask install vscode```
* [Docker](https://www.docker.com/get-started/) - Docker Containerization
* [Remote-dev extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) - Vscode extension for development inside docker container
 
### Prerequisites

What things you need to install the software and how to install them

```
vscode
docker
remote-dev extension
```

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
vscode <project folder name>
open project in container
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds
 
See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.
 