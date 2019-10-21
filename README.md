# Sesam Client

[![Build Status](https://travis-ci.org/tombech/sesam-py.svg?branch=master)](https://travis-ci.org/tombech/sesam-py)

Sesam command tool to use with [Sesam](https://sesam.io) The Hybrid Data Hub iPaaS.

## Usage

Typical workflow:

```
$ sesam wipe
$ sesam upload
Node config replaced with local config.
## edit stuff in Sesam Management Studio
$ sesam download
Local config replaced by node config.
$ sesam status
Node config is up-to-date with local config.
$ sesam run
Run completed.
$ sesam update
Current output stored as expected output.
$ sesam verify
Verifying output...passed!
```

Or run the full test cycle (typical CI setup):

```
$ sesam test
Node config replaced with local config.
Run completed.
Verifying output (1/3)...passed!
Run completed.
Verifying output (2/3)...passed!
Run completed.
Verifying output (3/3)...passed!
```

## Configuring tests

| Property | Description | Type | Required | Default |
|------------|----------------------------------------------------------------------------|------------------|----------|-----------------------------------------------------------------------|
| _id | Name of the test. | string | No | Name of the .test.json file |
| type | Config type so that this later can just be part of the rest of the config. | String | No | test |
| description | A description of the test. | string | No | |
| ignore | If the output should be ignored during tests. | boolean | No | ``false`` |
| endpoint | If the output should be fetched from a published endpoint instead. | string | No | By default the json is grabbed from ``/pipes/<my-pipe>/entities``
| stage | In which pipe stage to get the entities (source/before-transform/after-transform/sink) | string | No | By default the stage is ``sink``
| file | File that contains the expected results. | string | No | Name of the .test.json file without .test (e.g. foo.test.json looks for foo.json)
| pipe | Pipe that contains the output to test. | string | No | Same as above |
| blacklist | Properties to ignore in the output. | Array of strings | No | ``[]`` |
| parameters | Which parameters to pass as bound parameters. Note that parameters only works for published endpoints. | Object | No | ``{}`` |


Example: 
```
$ cat foo.test.json
{
  "_id": "foo",
  "type": "test",
  "file": "foo.json"
  "blacklist": ["my-last-updated-ts"],
  "ignore": false
}
```

### DTL parameters

If you need to pass various variations of bound parameters to the DTL, you just create multiple .test.json files for each combination of parameters.

Example:
```
$ cat foo-A.test.json
{
  "pipe": "foo",
  "file": "foo-A.xml",
  "endpoint": "xml",
  "parameters": {
    "my-param": "A"
  }
}
$ cat foo-B.test.json
{
  "pipe": "foo",
  "file": "foo-B.xml",
  "endpoint": "xml",
  "parameters": {
    "my-param": "B"
  }
}
```
This will compare the output of ``/publishers/foo/xml?my-param=A`` with the contents of ``foo-A.xml`` and ``/publishers/foo/xml?my-param=B`` with the contents of ``foo-B.xml``.

### Internal properties

All internal properties except ``_id`` and ``_deleted`` are removed from the output. Entities that has ``_deleted`` set to ``false`` will also be removed.

### Endpoints

By default the entities are fetched from ``/pipes/<my-pipe>/entities``, but if endpoint is set it will be fetched from
``/publishers/<my-pipe>/<endpoint-type>`` based on the endpoint type specified. Note that the pipe needs to be configured to publish to this endpoint.
 
Example:
```
{
  "_id": "foo",
  "type": "test",
  "endpoint": "xml",
  "file": "foo.xml"
}
```
This will compare the output of ``/publishers/foo/xml`` with the contents of ``foo.xml``.


Example:
```
{
  "_id": "foo",
  "type": "test",
  "endpoint": "json",
  "stage": "source"
}
```
This will compare the output of ``/pipes/foo/entities?stage=source`` with the contents of ``foo.json``, useful
when the pipe's sink strips away the "_id" property for example.


### Blacklisting

If the data contains values that are not deterministic (e.g. timestamp added during the run) they can be filtered out using the blacklist.
 
Example:
```
{
  "_id": "foo",
  "type": "test",
  "blacklist": ["foo", "ns1:bar"]
}
```

This will filter out properties called ``foo`` and ``ns1:bar`` (namespaced).
 
If the data is not located at the top level, a dotted notation is supported ``foo.bar``. This will remove the ``bar`` property from the object (or list of objects) located under the ``foo`` property. If you need to blacklist a property that actually contains a dot, the dot can be escaped like this ``foo\.bar``

If you need to ignore a property on a list of objects, you can also use this notation ``foos.*.bar``. This will remove the ``bar`` property from all the objects located under ``foos``.

Example:
```
{
  "_id": "foo",
  "foos": {
    "A": {
      "bar": "baz",
      "foobar": "foo"
    }
  }
}
```

Will end up as the following (with ``"blacklist": ["foos.*.bar"]``):
```
{
  "_id": "foo",
  "foos": {
    "A": {
      "foobar": "foo"
    }
  }
}
```
  
### Avoid ignore and blacklist

It is recommended to avoid ignoring or blacklisting as much as possible as this creates a false sense of correctness. Tests will pass, but deviations are silently ignored. A better solution is to avoid these properties in the output if possible.

### Scheduler customization

By default the upload command will add a test-friendly scheduler as part of the configuration. The ``_id`` for this micro service system is ``scheduler``, but it can be overridden with the flag "--scheduler-id my-scheduler-id" if you need to override this.
 
If you want to configure a custom scheduler manually as part of the configuration you need to enable the ``--custom-scheduler`` flag.

This custom scheduler needs to implement the following: 

1. POST /start (the tool will call this when the scheduler should start)
2. GET / (the tool will then poll this until it returns with state 'success' or 'failure')
```
{
  "state": "?|success|failure" 
}
```

## Installing

You can either run the sesam.py script directly using python, or you can download and run a stand alone 
binary from [Github Releases](https://github.com/tombech/sesam-py/releases/).


To install and run the sesam client with python on Linux/OSX (python 3.5+ required):
```
$ cd sesam
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ python sesam.py -version
sesam version 1.0.0
```

To create a sesam client binary with pyinstaller on Linux/OSX (python 3.5+ required):
```
$ cd sesam
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pyinstaller --onefile sesam.py
$ dist/sesam -version
sesam version 1.0.0
```
