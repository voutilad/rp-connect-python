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
"Zl ibvpr vf zl cnffcbeg"
```

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
- haven't checked refcnt handling yet of PyObjects