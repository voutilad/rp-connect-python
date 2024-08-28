# benchmarks!

## reverse
Take a given text input stored in a named field (i.e. `this.text`), reverse the
text and store it back to the same field (`root.text`).

### parameters
* 27 August, 2024
* v1.4.1
* 100m inputs of the same text blob:
    ```
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
    Vivamus faucibus est et metus sodales, vitae faucibus leo faucibus.
    Nunc sed dolor eu erat ullamcorper imperdiet quis in sem. Nullam in
    felis eget enim sollicitudin venenatis. Aliquam erat volutpat.
    Pellentesque vehicula odio ut leo egestas rhoncus. Sed non odio sit
    amet.
    ```
* `batch_size`: 10k
* `drop` all output
* Test system
  * Google Compute Engine `n2d-highcpu-16`
    * x86/64
    * 16 vCPU
    * 16 GB RAM
  * Ubuntu 24.04 LTS

### mapping
Via bloblang and the `mapping` processor:

```
$ time ./rp-connect-python run \
  -r benchmark/bloblang_reverse.yaml benchmark/bench_100m.yaml 

real    3m3.827s
user    24m34.662s
sys     0m28.041s

$ time ./rp-connect-python run \
  -r benchmark/bloblang_reverse.yaml benchmark/bench_100m.yaml 

real    3m3.146s
user    24m34.319s
sys     0m28.659s

$ time ./rp-connect-python run \
  -r benchmark/bloblang_reverse.yaml benchmark/bench_100m.yaml

real    3m1.022s
user    24m16.991s
sys     0m27.985s
```

Best Result: `553k messages/second`


### mutation
Via bloblang and the `mutation` processor:

```
$ time ./rp-connect-python run \
  -r benchmark/bloblang_mutation_reverse.yaml benchmark/bench_100m.yaml              


real    2m57.016s
user    23m33.495s
sys     0m26.683s
$ time ./rp-connect-python run \
  -r benchmark/bloblang_mutation_reverse.yaml benchmark/bench_100m.yaml 

real    2m56.845s
user    23m33.779s
sys     0m27.016s
$ time ./rp-connect-python run \
  -r benchmark/bloblang_mutation_reverse.yaml benchmark/bench_100m.yaml 

real    2m56.045s
user    23m20.608s
sys     0m26.129s
```

Best Result: `568k messages/second`


### python -- multi-interpreter mode
Via the `python` processor running in `multi` mode:

```
$ time ./rp-connect-python run \
  -r benchmark/python_reverse.yaml benchmark/bench_100m.yaml       

real    8m24.526s
user    124m3.090s
sys     2m13.124s
$ time ./rp-connect-python run \
  -r benchmark/python_reverse.yaml benchmark/bench_100m.yaml 

real    8m10.400s
user    120m41.567s
sys     2m8.110s
$ time ./rp-connect-python run \
  -r benchmark/python_reverse.yaml benchmark/bench_100m.yaml 

real    8m14.062s
user    121m43.118s
sys     2m7.804s
```

Best Result: `204k messages/second`

### python -- legacy-interpreter mode
Via the `python` processor running in `legacy` mode:

```
$ time ./rp-connect-python \
  run -r benchmark/python_legacy_reverse.yaml benchmark/bench_100m.yaml 

real    46m31.732s
user    104m34.426s
sys     1m35.958s
```

Best Result `35.8k messages/second`

### python -- single-interpreter mode
Via the `python` processor running in `single` mode:

```
$ time ./rp-connect-python \
  run -r benchmark/python_single_reverse.yaml benchmark/bench_100m.yaml 

TKTKTK
```

Best Result `TKTKT messages/second`
