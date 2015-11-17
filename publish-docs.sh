#!/bin/bash

branch=`git branch | grep '\*' | sed 's/\* *//'`

npm run generate-docs || exit 1
git checkout gh-pages || exit 1

cp -R doc-html/ doc || exit 1
git add doc/ || exit 1
git commit -m "Generate documentation for v$(node -e "console.log(require('./package').version)")"
git push origin gh-pages
git checkout $branch
git stash pop
