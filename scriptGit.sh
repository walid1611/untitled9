#!/bin/bash
read -p 'introduire le message du commmit' message
git status
git add .
git commit -m "test de script shell"
git push origin master