UID:=`id -u`
GID:=`id -g`
PACKAGE:=data_pipeline
DOCKER_RUN_LUCID:=docker run -t -v  $(CURDIR)/../:/work:rw $(PACKAGE)_lucid_container
DOCKER_RUN_TRUSTY:=docker run -t -v  $(CURDIR)/../:/work:rw $(PACKAGE)_trusty_container
DOCKER_QUICK_START:=docker run -t -i -v $(CURDIR)/../:/work:rw $(PACKAGE)_lucid_container

build_lucid_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/lucid/ && docker build -t "$(PACKAGE)_lucid_container" .
package_lucid: build_lucid_docker
	$(DOCKER_RUN_LUCID) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	$(DOCKER_RUN_LUCID) chown -R $(UID):$(GID) /work
itest_lucid: package_lucid
	$(DOCKER_RUN_LUCID) /work/yelp_package/itest/ubuntu.sh


build_trusty_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/trusty/ && docker build -t "$(PACKAGE)_trusty_container" .
package_trusty: build_trusty_docker
	$(DOCKER_RUN_TRUSTY) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	$(DOCKER_RUN_TRUSTY) chown -R $(UID):$(GID) /work
itest_trusty: package_trusty
	$(DOCKER_RUN_TRUSTY) /work/yelp_package/itest/ubuntu.sh

quick_start:
	$(DOCKER_QUICK_START) /bin/bash

clean:
	rm -rf dist/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
