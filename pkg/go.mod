module go.etcd.io/etcd/pkg/v3

go 1.23

toolchain go1.23.5

require (
	github.com/creack/pty v1.1.18
	github.com/dustin/go-humanize v1.0.1
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.10.0
	go.etcd.io/etcd/client/pkg/v3 v3.6.0-alpha.0
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.69.4
)

require (
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel v1.33.0 // indirect
	go.opentelemetry.io/otel/sdk v1.33.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250102185135-69823020774d // indirect
	google.golang.org/protobuf v1.36.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.etcd.io/etcd/client/pkg/v3 => ../client/pkg

// Bad imports are sometimes causing attempts to pull that code.
// This makes the error more explicit.
// Etcd contains lots of packages and dependency relationship.
// Shouldn't import unnecessary dependencies
replace (
	go.etcd.io/etcd => ./FORBIDDEN_DEPENDENCY
	go.etcd.io/etcd/api/v3 => ./FORBIDDEN_DEPENDENCY
	go.etcd.io/etcd/tests/v3 => ./FORBIDDEN_DEPENDENCY
	go.etcd.io/etcd/v3 => ./FORBIDDEN_DEPENDENCY
)
