#!/bin/bash

gunicorn -b 192.168.1.107:8001 -w 8 trade_api:application
