# docs: python -m SimpleHTTPServer 8001
docs: twistd -no web -p 8001 --path=.
kafka: tox -e devenv-command "$(make compose-prefix) kill" && tox -e devenv-command "$(make compose-prefix) rm --force" && tox -e devenv-command "$(make compose-prefix) up kafka schematizer"
