# analyze service

![neuromancer](neuromancer.jpeg)

## what is this? 

The analyze service is the static analysis step of the pipeline. In the alpha deployment it was [wintermute](https://gitlab.com/patchfox2/wintermute).

## how to a build it? 

`mvn clean install` 

Will build the service into a jar as well as create a docker image. 

## how to I make it go? 

The first thing you need to do is build the project. 

Then you'll need to spin up kafka and postgres. Do so by way of the docker-compose file thusly 

`docker-compose up` 

Then you can spin up the data-service from a new terminal thusly

`mvn spring-boot:run` 

## where can I get more information? 

This is a PatchFox [turbo](https://gitlab.com/patchfox2/turbo) service. Click the link for more information on what that means and what turbo-charged services provide to both developers and consumers. 
