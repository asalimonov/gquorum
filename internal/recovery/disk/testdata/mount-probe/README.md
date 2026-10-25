# Same-device nested mount qualification

`main.go.txt` is an inert captured Linux probe using the actual private disk owner.
It creates no Node, listeners or application and checks that device numbers match
while descriptor mount IDs differ. Validation must refuse that nested mount and
leave source bytes unchanged.

For the 2026-10-25 run, a temporary `main.go` copy was built with:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/gquorum-recovery-mount-probe ./internal/recoverydisk/testdata/mount-probe
```

The temporary `.go` file was removed after compilation. Using cached `alpine:3.23.3`,
a unique labeled Docker volume was mounted at `/fixture`; the probe's `setup`
argument created its private directories. A second invocation additionally mounted
the same volume at `/fixture/wal/artifact/nested` and ran the default check. Both
containers used `--rm --network none` and the compiled read-only `/probe` entrypoint.
No shared test image was rebuilt or replaced.

Result: PASS, device65025 matched and distinct mount identity refused. The labeled
volume and both containers were removed; subsequent label-filtered listings were
empty. Native Darwin artifact functional tests exercise Fstatfs mount detection;
ordinary runtime/simulation paths do not invoke recovery artifact traversal.
