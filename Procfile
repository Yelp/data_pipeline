docs: python -m SimpleHTTPServer 8001
kafka: tox -e devenv-command docker-compose kill && tox -e devenv-command "docker-compose rm --force" && tox -e devenv-command docker-compose up kafka
