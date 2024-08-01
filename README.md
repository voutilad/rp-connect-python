# Redpanda Connect -- Python Prototype

## build
```bash
CGO_ENABLED=0 go build
```

## run
```
python3 -m venv venv
. venv/bin/activate
pip install requests
deactivate
./rp-connect-python run test.yaml
```

## known issues/requirements
- tested on macOS and Linux (arm64 for both)
- needs a recent python3 (how recent? not sure yet)
- haven't checked refcnt handling yet of PyObjects