#!/usr/bin/env -S bash -l
. $HOME/.profile
. /opt/dockerd/docker_daemon.sh

export GITHUB_ACTOR=bigmansocial
export GITHUB_TOKEN=$GH_TOKEN
export VAULT_ADDR=https://secret-store-default.management.b-anti-social.io

git config --global --add safe.directory /__w/kafka-event-processor/kafka-event-processor

if [ -d .git-crypt ]; then
  echo "$GPG_KEY" | gpg --import -
  git crypt unlock
fi

git config --global url."https://$GITHUB_ACTOR:$GITHUB_TOKEN@github.com".insteadOf https://github.com
