module github.com/agentium-lab/Janus/sdk/go

go 1.25.8

replace github.com/agentium-lab/Janus/core => ../../core

require (
	github.com/agentium-lab/Janus/core v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
