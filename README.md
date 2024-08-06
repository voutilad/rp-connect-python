# Redpanda Connect -- Python Prototype

Adds a Python interpreter to Redpanda Connect, so you can
do things like:

```yaml
# rot13.yaml
input:
  stdin: {}

pipeline:
  processors:
    - python:
        exe: ./venv/bin/python3
        script: |
          import codecs
          msg = content().decode()
          root = codecs.encode(msg, "rot_13")

output:
  stdout: {}
```

```
$ echo My voice is my passport | ./rp-connect-python run --log.level=off rot13.yaml 
Zl ibvpr vf zl cnffcbeg
```

Currently, the sub-interpreter that runs your code provides two hooks back into
Redpanda Connect:

- `content()` -- similar to the bloblang function, it returns the `bytes` of a message
- `root` -- this is a `dict` in scope by default, so you can readily assign key/values,
  or you can replace it with a new object like `root = "junk"`

## Requirements
- Python 3.12
- `setuptools` (makes it so much easier to find libpython, just `pip install` it)
- Go 1.22-ish (1.20 at least I think)

## Build
Easy:
```bash
CGO_ENABLED=0 go build
```

## Demo
A simple demo using `requests` is provided in `test.yaml`:

```
python3 -m venv venv
. venv/bin/activate
pip install requests setuptools
deactivate
./rp-connect-python run test.yaml
```

## known issues/requirements
- tested on macOS/arm64 and Linux/{arm64,amd64}
- you can only use one Python binary across all Python processors