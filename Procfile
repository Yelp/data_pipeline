# docs: python -m SimpleHTTPServer 5000 
docs: twistd -no web -p 5000 --path=.
kafka: tox -e devenv-command docker-compose kill && tox -e devenv-command "docker-compose rm --force" && tox -e devenv-command docker-compose up kafka
