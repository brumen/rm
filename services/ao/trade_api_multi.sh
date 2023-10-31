#!/bin/bash

gunicorn -b localhost:8001 -w 8 trade_api:application
