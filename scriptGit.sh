#!/bin/bash
read message
git status
git add .
git commit -m "test de script shell"
git push origin master