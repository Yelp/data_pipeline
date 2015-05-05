# docs: python -m SimpleHTTPServer 8001
docs: twistd -no web -p 8001 --path=.
kafka: tox -e devenv-command docker-compose kill && tox -e devenv-command "docker-compose rm --force" && tox -e devenv-command docker-compose up kafka
