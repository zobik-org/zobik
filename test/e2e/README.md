# test/e2e

One end-to-end test per development stage, each against a real container engine. They build the `zobik:dev` image and a development binary, bring up a network with a random name and remove it at the end.

```
go test -tags e2e ./test/e2e/
```
