# Tado Experience Day - InfluxDb Backend

## What is this?
This is a repository by tado to facilitate Experience Days with Software Engineering job candidates.
It is a backend server application using Grails framework.
It has a dependency on a timeseries database called InfluxDb, which can be set up with docker.

## Setting up development environment

### Pre-requisites
- `docker` on terminal
- `docker-compose` on terminal
- `Intellij IDEA - Ultimate`

Docker is needed for InfluxDb dependency, while Ultimate edition of Intellij IDE is needed to support Grails framework.

### Setting up the IDE
#### Import the project
- File > Open > Choose project folder

#### Create a Run Configuration:
- Run > Edit Configuration > + icon at the top left > Grails
- Set Application to what you
- Set Command line to 'run-app'
- Tick on 'Launch browser'

### Spinning up the project
- `$repo/ops> docker-compose up`. This spins up InfluxDb database.
- in the IDE, next to Play icon at the top, select the Run Configuration you created earlier.
- Hit Play icon. Wait for a minute.
- Verify that browser launched at localhost:8081 with 'Welcome to Grails' page.