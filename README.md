BUbiNG
======

[![Build Status](https://travis-ci.org/LAW-Unimi/BUbiNG.svg?branch=master)](https://travis-ci.org/LAW-Unimi/BUbiNG)

This is the public repository of [BUbiNG](http://law.di.unimi.it/software.php#bubing), the next generation web crawler from the [Laboratory of Web Algorithmics](http://law.di.unimi.it); on the lab website you can find the  [API](http://law.di.unimi.it/software/bubing-docs/) and [configuration](http://law.di.unimi.it/software/bubing-docs/overview-summary.html#overview.description) documentation, and instructions on [how to stop](http://law.di.unimi.it/BUbiNG.html) from accessing your site.

# Docker

Run pulsar service and admin dashboard

    docker-compose up -d

Create a first account for dashboard

    CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
    curl \
        -H "X-XSRF-TOKEN: $CSRF_TOKEN" \
        -H "Cookie: XSRF-TOKEN=$CSRF_TOKEN;" \
        -H 'Content-Type: application/json' \
        -X PUT http://localhost:7750/pulsar-manager/users/superuser \
        -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'
