#!/bin/bash

ps aux | grep goreman | grep -v ssh | grep -v $$ | awk '{print $2}' | xargs kill