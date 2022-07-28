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


# Usage

### one message from Pulsar
Topic client : "{pulsarFrontierToCrawlURLsTopic}-<id>"

Call of : CrawlRequestsReceiver.received
 - 1 : Test if the request is out of date
 - 2 : Add the crawler request to Queue : Frontier.receivedCrawlRequests

### Distributor Thread 

Distributor is one thread start by Frontier

Call : Distributor.processURL(crawlRequest)
- 1 : Test if the request is out of date
- 2 : Find a VisitState into Distributor.schemeAuthority2VisitState
  - if VisitState list is empty
    - add a new VisitState
    - enqueue a Robots request before the request
    - add VisitState into Distributor.schemeAuthority2VisitState
    - add crawlRequest into : Frontier.newVisitStates for DNS threads
  - if a VisitState already exist for this domain
    - manage the limit number of request for the same domain in memory or in disk
    - enqueue crawlRequest into VisitState or Frontier.virtualizer for disk


### DNS management

- 1 : Resolve host : dnsResolver.resolve(host)
- 2 : Test if ip is into blacklist and purge VisitState in this case
- 3 : Get or create a WorkbenchEntry for the host IP
- 4 : Add VisitState into WorkbenchEntry (that call WorkbenchEntry.add -> Workbench.add into Workbench.entries)

### Fetching

TodoThread -> loop on : frontier.workbench.acquire() (from Workbench.entries)
TodoThread -> put VisitState into todo queue

FetchingThread -> getNextVisitState() return one VisitState from todo queue
FetchingThread -> processVisitState()





Topic for results : "{pulsarFrontierFetchTopic}-{0->pulsarFrontierTopicNumber}"
