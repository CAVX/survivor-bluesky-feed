#!/bin/bash
## build.sh

yarn build

eval "sed '1 i\\#\\!$(which node)' dist/index.js > dist/index2.js"
mv dist/index2.js dist/index.js
chmod +x dist/index.js
