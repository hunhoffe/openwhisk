#!/bin/bash
wsk action create hello hello.js
wsk action invoke hello
wsk action invoke hello --result
