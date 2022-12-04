#!/bin/bash
set -e

mongo <<EOF
use app
db.createUser({
  user:  '$MONGODB_ROOT_USERNAME',
  pwd: '$MONGODB_ROOT_PASSWORD',
  roles: ['readWrite']
})
EOF