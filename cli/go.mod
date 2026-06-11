module github.com/agentium-lab/Janus/cli

go 1.25.8

require (
	github.com/agentium-lab/Janus/core v0.0.0
	github.com/agentium-lab/Janus/sdk/go v0.0.0
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/agentium-lab/Janus/core => ../core
	github.com/agentium-lab/Janus/sdk/go => ../sdk/go
)
