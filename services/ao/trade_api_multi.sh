#!/bin/bash

gunicorn -b localhost:8000 -w 8 trade_api:application
