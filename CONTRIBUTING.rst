============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://jira.yelpcorp.com/browse/DATAPIPE/,
on the DATAPIPE project.

If you are reporting a bug, please include:

* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs or Implement Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Look through the Jira issues. Anything
is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

Data Pipeline Clientlib could always use more documentation, whether as part of the
official Data Pipeline Clientlib docs, in docstrings, or even on trac.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at
https://jira.yelpcorp.com/browse/DATAPIPE/ on the
DATAPIPE project.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that contributions are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `data_pipeline` for
local development.

1. Clone the `data_pipeline` repo::

    $ git clone git@git.yelpcorp.com:clientlibs/data_pipeline

2. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

Now you can make your changes locally.

See :doc:`index` for information about setting up TDD tools.

3. When you're done making changes, check that your changes pass style and unit
   tests, including testing other Python versions with tox::

    $ tox

To get tox, just pip install it.

4. Commit your changes and push your branch::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

5. Get a code review::

    $ review-branch

Contribution Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The change should include tests.
2. If the change adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 2.6, 2.7, and 3.3, and for PyPy.
   Run the ``tox`` command and make sure that the tests pass for all supported
   Python versions.

Building Tools
--------------

Before you submit a pull request, make sure that any new tool meets these guidelines:

1. Your tool is runnable from the commandline 
   (with python or on it's own) in the data_pipeline/tools folder
2. Any python libraries your tool requires is in both 
   requirements-tools.txt and in setup.py in extras_require["tools"]
3. In bin, create a script that can be run from the commandline that runs your tool.
   See the others in the folder as an example on what to do with a python batch.
   Put the path to this new script in setup.py under scripts
4. In debian/data-pipeline-tools.links create a new link. Use the others there as an example.
5. In yelp_package/itest/ubuntu.sh, add a line to the string SCRIPTS 
   with the name of the script you put in bin

Tips
----

To run a subset of tests::

     $ py.test tests/data_pipeline_test.py
