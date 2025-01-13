#!/bin/bash

gunicorn -b 192.168.1.107:8000 -w 8 trade_api:application
