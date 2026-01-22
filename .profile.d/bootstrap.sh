#!/usr/bin/env bash

if [ -n "$START_DATABASE" ]; then
  bash -c "$START_DATABASE" || true
fi
