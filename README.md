# Redpanda Connect + Python
[![Build without CGO_ENABLED](https://github.com/voutilad/rp-connect-python/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/voutilad/rp-connect-python/actions/workflows/build.yml)

<div align="center">
  <img src="./rpcn_and_python.jpg" width="45%" alt="A redpanda & a python sipping tea together as friends.">
</div>

Adds an embedded Python interpreter to Redpanda Connect, so you can
write your transformation logic in pure Python:

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

Currently, the (sub-)interpreter that runs your code provides two hooks back into
Redpanda Connect, mimicking Bloblang behavior:

- `content()` -- similar to the Bloblang function, it returns the `bytes` of a message.
- `root` -- this is a `dict`-like object in scope by default, so you can readily assign
  key/values like a Python `dict` _or_ use Bloblang-like assignment by attribute
  (e.g. `root.name.first = "Dave"`). Or...you can reassign it: `root = (1, 2)`.

## Requirements
- Python 3.12
- `setuptools` (makes it so much easier to find `libpython`, just `pip install` it.)
- Go 1.22 or newer

## Build
Easy:
```bash
CGO_ENABLED=0 go build
```

## Demo
A simple demo using `requests` is provided in `test.yaml` which will enrich a
message with a callout to an external web service. In this case, a public api
for echoing your IPv4 back to you.

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
          root["msg"] = f"You said: '{msg}'"
          root["at"] = time.ctime()
          try:
            root["ip"] = requests.get("https://api.ipify.org").text
          except:
            root["ip"] = "no internet?"

output:
  stdout: {}
```

To run the demo:

```sh
python3 -m venv venv
. venv/bin/activate
pip install requests setuptools
deactivate
./rp-connect-python run test.yaml
```

## Interpreter Modes
This processor now supports multiple interpreter modes that may be set
separately on each processor instance.

- `multi`:  The default. Uses multiple sub-interpreters with their own
            memory allocators and GILs.
- `legacy`: Uses multiple sub-interpreters, but configured to use a
            single GIL and memory allocator.
- `single`: Uses a single interpreter (i.e. no sub-interpreters) for
            all execution.

### Multi & Legacy Modes
Most pure Python code should "just work" with `multi` mode and `legacy`
mode. Some older Python extensions, written in C or the like, may not
work in `multi` mode and require `legacy` mode.

If you see issues using `multi` (e.g. crashes), switch to `legacy`.

In some cases, `legacy` can perform as well or slightly better than
`multi` even though it uses a single GIL. It's very workload
dependent.

### Single Mode
Using `single` mode for a runtime will execute the Python code in the
context of the "main" interpreter. (In `multi` and `legacy` modes,
sub-interpreters derive from the main interpreter.)

While you may scale out your `single` mode processors, only a single
instance may utilize the interpreter at a time. (Hence, the name
`single`.)

A lot of scientific software that uses external non-Python native code
may run best in `single` mode. This includes:

- `numpy`
- `pandas`

## Known Issues / Limitations
- Tested on macOS/arm64 and Linux/{arm64,amd64}.
  - Not expected to work on Windows. Requires `gogopython` updates.
- You can only use one Python binary across all Python processors.
- `pandas` sometimes has issues starting on arm64 machines.
- Hardcoded still for Python 3.12. Should be portable to 3.13 and,
  in cases of `single` mode, earlier versions. Requires changes to
  `gogopython` I haven't made yet.

## Python Compatability
This is en evolving list of notes/tips related to using certain
popular Python modules:

### `requests`
Works best in `legacy` mode. Known to cause deadlocks on shutdown
in `single` mode. Currently, can panic `multi` mode on some systems.

> While `requests` is pure Python, it does hook into some modules that
> are not. Still identifying a race condition causing memory corruption
> in `multi` mode.

### `numpy`
Recommends `single` mode as explicitly does not support Python
sub-interpreters (_a la_ `multi` or `legacy` modes). May work in
`legacy`, but be careful.

### `pandas`
Seems to work best on `amd64` systems and in `legacy` mode. Depends on
`numpy`, so might be best used in `single` mode if stability is a
concern.

### `pillow`
Seems to work ok in `legacy` mode, but doesn't support sub-interpreters,
so recommended to run in `single` mode.

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
        mode: single
        script: |
          from PIL import Image
          from io import BytesIO

          infile = BytesIO(content())
          try:
            with Image.open(infile) as im:
              root.format = im.format
              root.size = im.size
              root.mode = im.mode
          except OSError:
            pass
    - mapping: |
        root = this
        root.path = metadata("path")

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

## License and Supportability
Source code in this project is licensed under the Apache v2 license unless
noted otherwise.

This software is provided without warranty or support. It is not part of
Redpanda Data's enterprise offering and not supported by Redpanda Data.
