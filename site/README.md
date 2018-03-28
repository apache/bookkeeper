# The BookKeeper Website

Welcome to the code for the Apache BookKeeper website! Instructions on building the site and running it locally can be found here in this `README`.

## Tools

The site is built using Jekyll and Sass.

> I'll provide more specific info here later.

## Prerequisities

In order to run the site locally, you need to have the following installed:

* Ruby 2.3.1 and Rubygems
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

## Staging website for reviews

When you submit a pull request for modifying website or documentation, you are recommended to make your changes live for reviews.

Here are a few steps to follow to stage your changes:

1. You need to create a github repo called `bookkeeper-staging-site` under your github account. You can fork this [staging repo](https://github.com/sijie/bookkeeper-staging-site) as well.
2. In your `bookkeeper-staging-site` repo, go to `Settings > GitHub Pages`. Enable `GitHub Pages` on `master branch /docs folder`.
3. Make changes to the website, follow the steps above to verify the changes locally.
4. Once the changes are verified locally, you can run `make staging`. It will generate the files under `site/local-generated`.
5. Run `scripts/staging-website.sh`. It would push the generated website to your `bookkeeper-staging-site`.
6. Your changes will be live on `https://<your-github-id>.github.io/bookkeeper-staging-site`.

If you have any ideas to improve the review process for website, please feel free to contact us at dev@bookkeeper.apache.org.

