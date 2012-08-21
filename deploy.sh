#!/usr/bin/env bash
mvn deploy -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-snapshots-local
