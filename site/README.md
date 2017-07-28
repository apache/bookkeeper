# The BookKeeper Website

Welcome to the code for the Apache BookKeeper website! Instructions on building the site and running it locally can be found here in this `README`.

## Tools

The site is built using Jekyll and Sass.

> I'll provide more specific info here later.

## Prerequisities

In order to run the site locally, you need to have the following installed:

* Ruby 2.4.1 and Rubygems
* The Javadoc CLI tool

## Setup

```shell
$ make setup
```

## Building the site

```shell
$ make build
```

Please note that this will *not* build the Javadoc. That requires a separate command:

```shell
$ make javadoc
```

## Serving the site locally

To run the site in interactive mode locally:

```shell
$ make serve
```

Then navigate to `localhost:4000`. As you make changes, the browser will auto-update.
