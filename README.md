# Redpanda Connect + Python
[![Build without CGO_ENABLED](https://github.com/voutilad/rp-connect-python/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/voutilad/rp-connect-python/actions/workflows/build.yml)

<div align="center">
  <img src="./rpcn_and_python.jpg" width="45%" alt="A redpanda & a python sipping tea together as friends.">
</div>

Adds an embedded Python interpreter to Redpanda Connect, so you can
write your integration and transformation logic in pure Python:

```yaml
# rot13.yaml
input:
  stdin: {}

pipeline:
  processors:
    - python:
        exe: python3
        script: |
          import codecs
          msg = content().decode()
          root.original = msg
          root.encoded = codecs.encode(msg, "rot_13")

output:
  stdout: {}

logger:
  level: OFF
```

```
$ echo My voice is my passport | ./rp-connect-python run examples/rot13.yaml
{"original": "My voice is my passport", "encoded": "Zl ibvpr vf zl cnffcbeg"}
```


## Requirements
- Python 3.12 (hard requirement, currently!)
- `setuptools` (makes it so much easier to find `libpython`, just `pip install`
  it.)
  - On macOS, if you used `brew` to install Python, it can fall back to using
    `otool` to find the dynamic library.
  - On Linux...sorry! You must use `setuptools`.
- Go 1.22 or newer

## Building
Building `rp-connect-python is simple as it's using pure Go code:

```bash
CGO_ENABLED=0 go build
```

That's it! A variety of tests are provided and looking at the current GitHub
action [file](./.github/workflows/build.yml) shows some examples.


## Component Types
This project provides the following new Python component types:

1. [Input](#input) -- for generating data using Python
2. [Processor](#processors) -- for transforming data with Python
3. [Output](#output) -- for sinking data with Python


## Input
The `python` input allows you to generate or acquire data using Python. Your
script can provide one of the following data generation approaches based on
the type of object you target when setting the `name` configuration property:

- `object`
  - If you provide a single Python object, it can be passed as a single input.
- `list` or `tuple`
  - A list or tuple will have each item extracted and provided to the pipeline.
- `generator`
  - Items will be produced from the generator until it's exhausted.
- `function`
  - Any function provided will be called repeatedly until it returns `None`.
  - Functions may take an optional kwarg `state`, a `dict`, and use it
    to keep state between invocations.

### Input Serialization
By default, the input will serialize data either as native Go values (in the
case of `string`, `number`, `bytes`) and will convert to JSON in the case of
Python container types `dict`, `list`, and `tuple`.

Serialization via `pickle` can be done manually, but if you set `pickle: true`
the input will convert the produced Python object using `pickle.dumps()`
automatically, storing the output as raw bytes on the Redpanda Connect
`Message`.

### Input Configuration
Common configuration with defaults for a Python `input`:

```yaml
input:
  label: ""
  python:
    pickle: false     # Enable pickle serializer
    batch_size: 1     # How many messages to include in a single message batch.
    mode: global      # Interpreter mode (one of "global", "isolated", "isolated_legacy")
    exe: "python3"    # Name of python binary to use.
    name:             # No default (required), name of generating local object.
    script:           # No default (required), Python code to execute.
```

An example that uses a Python generator to emit 10 records, one every second:

```yaml
input:
  python:
    name: g
    script: |
      import time
      def producer():
        for i in range(10):
          time.sleep(1)
          yield { "now": time.ctime(), "i": i }
      g = producer()
```

### Input Caveats
Currently, a single interpreter is used for executing the input script. If you
change the [mode](#interpreter-modes), it will use different interpreter
settings which could affect [python compatability](#python-compatability) of
your script. Keep this in mind.


## Processor
The `python` processor provides a similar experience to the `mapping` bloblang
processor, but in pure Python. The interpreter that runs your code provides
lazy hooks back into Redpanda Connect, to mimic bloblang behavior:

- `content()` -- similar to the bloblang function, it returns the `bytes` of
  a message. This performs a lazy copy of raw bytes into the interpreter.

- `metadata(key)` -- similar to the bloblang function, it provides access to
  the metadata of a message using the provided `key`.

- `root` -- this is a `dict`-like object in scope by default providing three
  operating modes simultaneously:
  - Assign key/values like a Python `dict`, e.g. `root["name"] = "Dave"`
  - Use bloblang-like assignment by attribute, e.g. `root.name.first = "Dave"`
  - Reassign it to a new object, e.g. `root = (1, 2)`. (Note: if you reassign
    `root`, it loses its magic properties!)

> Heads up!
>
> If using the bloblang-like assignment, it will create the hierarchy of keys
> similar to in bloblang. `root.name.first = "Dave" will work even if "name"
> hasn't been assigned yet, producing a dict like:
> ```python
> root = { "name": { "first": "Dave" } }
> ```

For the details of how `root` works, see the `Root` Python
[class](./processor/globals.py).

Additionally, the following helper functions and objects improve
interoperability:

- `unpickle()` -- will use `pickle.loads()` to deserialize the Redpanda Connect
  `Message` into a Python object.

- `meta` -- a `dict` that allows you to assign new metadata values to a message
  or delete values (if you set the value to `None` for a given key).

An example using `unpickle()`:

```yaml
pipeline:
  processors:
    - python:
        script: |
          # these are logically equivalent
          import pickle
          this = pickle.loads(content())

          this = unpickle()

          root = this.call_some_method()

          # if relying on Redpanda Connect structured data, use JSON.
          import json
          this = json.loads(content().decode())

          root = this["a_key"]
```

> The processor does not currently support automatic deserialization of
> incoming data in an effort to keep as much of the expensive hooks back into
> Go as lazy as possible so you only pay for what you use.

## Processor Configuration
Common configuration with defaults for a Python `processor`:

```yaml
pipeline:
  processors:
    - python:
        exe: "python3"  # Name of python binary to use.
        mode: "global"  # Interpreter mode (one of "global", "isolated", "isolated_legacy")
        script:         # No default (required), Python script to execute
```

## Processor Demo
A simple demo using [requests](./examples/requests.yaml) which will enrich a
message with a callout to an external web service illustrates many of the prior
concepts of using a Python processor:

```yaml
input:
  generate:
    count: 3
    interval: 1s
    mapping: |
      root.title = "this is a test"
      root.uuid = uuid_v4()
      root.i = counter()

pipeline:
  processors:
    - python:
        exe: ./venv/bin/python3
        script: |
          import json
          import requests
          import time

          data = content()
          try:
            msg = json.loads(data)["title"]
          except:
            msg = "nothing :("
          root.msg = f"You said: '{msg}'"
          root.at = time.ctime()
          try:
            root.ip = requests.get("https://api.ipify.org").text
          except:
            root.ip = "no internet?"

output:
  stdout: {}
```

To run the demo, you need a Python environment with the `requests` module
installed. This is easy to do with a virtual environment:

```shell
# Create a new virtual environment.
python3 -m venv venv

# Update pip, install setuptools, and install requests into the virtual env.
./venv/bin/pip install --quiet -U pip setuptools requests

# Run the example.
./rp-connect-python run --log.level=off examples/requests.yaml
```

You should get output similar to:

```
{"msg": "You said: 'this is a test'", "at": "Fri Aug  9 19:07:29 2024", "ip": "192.0.1.210"}
{"msg": "You said: 'this is a test'", "at": "Fri Aug  9 19:07:30 2024", "ip": "192.0.1.210"}
{"msg": "You said: 'this is a test'", "at": "Fri Aug  9 19:07:31 2024", "ip": "192.0.1.210"}
```


## Output
Presently, the Python `output` is a bit of a hack and really just a Python
`processor` configured to use a single interpreter instance.

This means all the configuration and behavior is the same as in the
[processor configuration](#processor-configuration).

When the `output` improves and warrants further discussion, check this space!

For now, a simple [example](./examples/output.yaml) that simply writes the
provided message to `stdout`:

```yaml
input:
  generate:
    count: 5
    interval:
    mapping: |
      root = "hello world"

output:
  python:
    script: |
      msg = content().decode()
      print(f"you said: '{msg}'")

http:
  enabled: false
```


## Interpreter Modes
`rp-connect-python` now supports multiple interpreter modes that may be set
separately on each `input`, `processor`, and `output` instance.

- `global` (the default)
  - Uses a global interpreter (i.e. no sub-interpreters) for all execution.
  - Allows passing pointers to Python objects between components, avoiding
    costly serialization/deserialization.
  - Provides the most compatability at expense of throughput as your code will
    rely on the global main interpreter for memory management and the GIL.

- `isolated`
  - Uses multiple isolated sub-interpreters with their own memory allocators
    and GILs.
  - Provides the best throughput performance for pure-Python use cases that
    don't leverage Python modules that use native code (e.g. `numpy`).
  - Require serializing/deserializing data as it leaves the context of the
    interpreter.

- `isolated_legacy`
  - Same as `isolated`, but instead of distinct GIL and memory allocators, uses
    a shared GIL and allocator.
  - Balances compatability with performance. Some Python modules might not
    support full isolation, but _will_ work in a shared GIL mode.


A more detailed discussion for the nerds follows.

### Isolated & Isolated Legacy Modes
Most pure Python code should "just work" with `isolated` mode and
`isolated_legacy` mode. Some older Python extensions, written in C or the
like, may not work in `isolated` mode and require `isolated_legacy` mode.

If you see issues using `isolated` (e.g. crashes), switch to
`isolated_legacy`.

> In general, crashes should _not_ happen. The most common causes are bugs
> in `rp-connect-python` related to _use-after-free_'s in the Python
> integration layer. If it's not that, it's an interpreter state issue,
> which is also a bug most likely in `rp-connect-python`. However, given the
> immaturity of multi-interpreter support in Python, if the issue "goes away"
> by switching modes (e.g. to "legacy"), it's possible it's deeper than just
> `rp-connect-python`.

In some cases, `isolated_legacy` can perform as well or _slightly better_ than
`isolated` even though it uses a shared GIL. It's very workload dependent, so
it's worth experimenting.

### Global Mode
Using `glopbal` mode for a runtime will execute the Python code in the
context of the "main" interpreter. (In `isolated` and `isolated_legacy` modes,
sub-interpreters derive from the "main" interpreter.) This is the traditional
method of embedding Python into an application.

While you may scale out your `global` mode components, only a single
component instance may utilize the "main" interpreter at a time. This is
irrespective of the GIL as Python's C implementation relies heavily on
thread-local storage for interpreter state.

> Go was design by people that think programmers can't handle managing
> threads. (Multi-threading is hard, but that's why we're paid the big
> bucks, right?) As a result, the Go runtime does its own scheduling of Go
> routines to some number of OS threads to achieve parallelism and
> concurrency. Python does not jibe with this and the vibes are off, so a
> lot of the `rp-connect-python` internals are for managing how to
> couple Python's thread-oriented approach with Go's go-routine world.

A lot of scientific software that uses external non-Python native code
may run best in `global` mode. This includes, but is not limited to:

- `numpy`
- `pandas`
- `pyarrow`

A benefit to `global` mode is it's one interpreter state across all components,
so you can create a Python object in one component (e.g. an `input`) and
easily use it in a `processor` stage without mucking about with serialization.
This is great for workloads that create large, in-memory objects, like Pandas
DataFrames or PyArrow Tables. In these cases, avoiding serialization may mean
`global` mode is more efficient even if there's fighting over the interpreter
lock.

> The current design assumes arbitrary Go routines will need to acquire
> ownership of the global ("main") interpreter and fight over a mutex. It's
> entirely possible the mutex is held at points where the GIL is actually
> released or releasable, meaning other Python code _could_ run safely. It's
> future work to figure out how to orchestrate this efficiently.

## Python Compatability
This is en evolving list of notes/tips related to using certain
popular Python modules:

### `requests`
Works best in `isolated_legacy` mode. Currently, can panic `isolated` mode on
some systems.

> While `requests` is pure Python, it does hook into some modules that
> are not. Still identifying a race condition causing memory corruption
> in `isolated` mode.

### `numpy`
Recommends `global` mode as explicitly does not support Python
sub-interpreters. May work in `isolated_legacy`, but be careful.

### `pandas`
Depends on `numpy`, so might be best used in `global` mode if stability is a
concern. Works fine with the `pickle` support for passing DataFrames, but might
not be the most efficient way for passing data around a long pipeline, so
`global` might be preferable to isolated modes.

An [example](./examples/pandas.yaml) that shows filtering a DataFrame and using
`pickle` to pass it from the `input` to the `processor`:

```yaml
input:
  python:
    mode: global
    name: df
    pickle: true
    script: |
      import pandas as pd
      df = pd.DataFrame.from_dict({"name": ["Maple", "Moxie"], "age": [8, 3]})

pipeline:
  processors:
    - python:
        mode: global
        script: |
          import pickle
          df = unpickle()
          root = df.to_dict("list")

output:
  stdout: {}
```

> Note the use of `mode: global`!

### `pyarrow`
Works fine in `global` mode. Might provide a better means of accessing large
datasets lazily vs. Pandas.

### `pillow`
Seems to work ok in `isolated_legacy` mode, but doesn't support
sub-interpreters, so recommended to run in `global` mode.

An example of a directory scanner that identifies types of JPEGs:

```yaml
input:
  file:
    paths: [ ./*.jpg ]
    scanner:
      to_the_end: {}

pipeline:
  processors:
    - python:
        exe: ./venv/bin/python3
        mode: global
        script: |
          from PIL import Image
          from io import BytesIO

          infile = BytesIO(content())
          try:
            with Image.open(infile) as im:
              root.format = im.format
              root.size = im.size
              root.mode = im.mode
              root.path = metadata("path")
          except OSError:
            pass

output:
  stdout: {}
```

Assuming you `pip install` the dependencies of `setuptools` and `pillow`:

```
$  python3 -m venv venv
$  ./venv/bin/pip install --quiet -U pip setuptools pillow
$  ./rp-connect-python run --log.level=off examples/pillow.yaml
{"format":"JPEG","mode":"RGB","path":"rpcn_and_python.jpg","size":[1024,1024]}
```


## Known Issues / Limitations
- Tested on macOS/arm64 and Linux/{arm64,amd64}.
    - Not expected to work on Windows. Requires `gogopython` updates.
- You can only use one Python binary across all Python processors.
- Hardcoded still for Python 3.12. Should be portable to 3.13 and,
  in cases of `global` mode, earlier versions. Requires changes to
  `gogopython` I haven't made yet.


## License and Supportability
Source code in this project is licensed under the Apache v2 license unless
noted otherwise.

This software is provided without warranty or support. It is not part of
Redpanda Data's enterprise offering and not supported by Redpanda Data.
