====
TODO
====

This file is available in your project at TODO.rst.  Hint::

    $ cat data_pipeline/TODO.rst | pastebinit

And send this to your releng deputy.

Ask your releng deputy to:
--------------------------

Create a git repo
+++++++++++++++++

Directions are available at https://trac.yelpcorp.com/wiki/RelengDeputies#CreateGitrepositories1.
At the time of the writing, to create this repo use the command::

    $ fab_repo setup_all:clientlibs/data_pipeline,owner=bam

Setup Promote to Pypi in Jenkins
++++++++++++++++++++++++++++++++

See https://trac.yelpcorp.com/wiki/Jenkins/PromoteToPypi

Add This Repo to Servicedocs
++++++++++++++++++++++++++++

See https://trac.yelpcorp.com/wiki/ServicedocsRunbook

You should:
-----------

After the Remote Git Repo is Created:
+++++++++++++++++++++++++++++++++++++

Add the remote repo and push your first branch::

    $ cd data_pipeline
    $ git init
    $ git remote add origin git@git.yelpcorp.com:clientlibs/data_pipeline
    $ git pull origin master
    $ git checkout -b initial-branch
    $ git add .
    $ git commit
    $ git push origin HEAD

Get Started by:
+++++++++++++++

* Write some code.
* Test it using `tox` or `make test`.
* Get it reviewed using `review-branch`, which should be setup.
* Get a pushmaster to push your changes to master, it should automatically
  be added to Pypi.

