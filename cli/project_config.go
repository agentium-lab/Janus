package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	defaultProjectFileName = "janus.project.yaml"
	projectConfigVersion   = "v1"
)

var projectFile string

type ProjectConfig struct {
	Version       string                    `yaml:"version"`
	DefaultTenant string                    `yaml:"default_tenant,omitempty"`
	Defaults      ProjectDefaults           `yaml:"defaults,omitempty"`
	Tenants       map[string]*ProjectTenant `yaml:"tenants"`
}

type ProjectDefaults struct {
	Protocol       string                 `yaml:"protocol,omitempty"`
	Classification string                 `yaml:"classification,omitempty"`
	Mailbox        ProjectMailboxDefaults `yaml:"mailbox,omitempty"`
	Capacity       ProjectCapacity        `yaml:"capacity,omitempty"`
	Policy         ProjectPolicyDefaults  `yaml:"policy,omitempty"`
}

type ProjectMailboxDefaults struct {
	ACKWaitSeconds   int `yaml:"ack_wait_seconds,omitempty"`
	MaxDeliver       int `yaml:"max_deliver,omitempty"`
	RetentionSeconds int `yaml:"retention_seconds,omitempty"`
}

type ProjectCapacity struct {
	MaxConcurrency int `yaml:"max_concurrency,omitempty"`
}

type ProjectPolicyDefaults struct {
	Priority int `yaml:"priority,omitempty"`
}

type ProjectTenant struct {
	Name     string                   `yaml:"name,omitempty"`
	Agents   map[string]*ProjectAgent `yaml:"agents,omitempty"`
	Budgets  ProjectBudgets           `yaml:"budgets,omitempty"`
	Policies ProjectPolicies          `yaml:"policies,omitempty"`
}

type ProjectAgent struct {
	Name         string              `yaml:"name,omitempty"`
	Team         string              `yaml:"team,omitempty"`
	Protocol     string              `yaml:"protocol,omitempty"`
	Endpoint     string              `yaml:"endpoint,omitempty"`
	Description  string              `yaml:"description,omitempty"`
	Capabilities []ProjectCapability `yaml:"capabilities,omitempty"`
	Concurrency  int                 `yaml:"concurrency,omitempty"`
	Capacity     ProjectCapacity     `yaml:"capacity,omitempty"`
	RPM          int                 `yaml:"rpm,omitempty"`
	TPM          int                 `yaml:"tpm,omitempty"`
	Mailbox      *ProjectMailbox     `yaml:"mailbox,omitempty"`
}

type ProjectCapability struct {
	ID                  string   `yaml:"id,omitempty"`
	Description         string   `yaml:"description,omitempty"`
	DataClassifications []string `yaml:"data_classifications,omitempty"`
}

func (c *ProjectCapability) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		c.ID = strings.TrimSpace(value.Value)
		return nil
	case yaml.MappingNode:
		type raw ProjectCapability
		var out raw
		if err := value.Decode(&out); err != nil {
			return err
		}
		*c = ProjectCapability(out)
		return nil
	default:
		return fmt.Errorf("capability must be a string or object")
	}
}

func (c ProjectCapability) MarshalYAML() (interface{}, error) {
	if c.Description == "" && len(c.DataClassifications) == 0 {
		return c.ID, nil
	}
	type raw ProjectCapability
	return raw(c), nil
}

type ProjectMailbox struct {
	ID               string `yaml:"id,omitempty"`
	Concurrency      int    `yaml:"concurrency,omitempty"`
	ACKWaitSeconds   int    `yaml:"ack_wait_seconds,omitempty"`
	MaxDeliver       int    `yaml:"max_deliver,omitempty"`
	RetentionSeconds int    `yaml:"retention_seconds,omitempty"`
}

type ProjectBudgets struct {
	Tenant         *ProjectBudgetLimits           `yaml:"tenant,omitempty"`
	Teams          map[string]ProjectBudgetLimits `yaml:"teams,omitempty"`
	Agents         map[string]ProjectBudgetLimits `yaml:"agents,omitempty"`
	Models         map[string]ProjectBudgetLimits `yaml:"models,omitempty"`
	ModelProviders map[string]ProjectBudgetLimits `yaml:"model_providers,omitempty"`
	Tasks          map[string]ProjectBudgetLimits `yaml:"tasks,omitempty"`
}

type ProjectBudgetLimits struct {
	RPM         int     `yaml:"rpm,omitempty"`
	TPM         int     `yaml:"tpm,omitempty"`
	Concurrency int     `yaml:"concurrency,omitempty"`
	DailyUSD    float64 `yaml:"daily_usd,omitempty"`
	MonthlyUSD  float64 `yaml:"monthly_usd,omitempty"`
}

type ProjectPolicies struct {
	Approve            ProjectApprovalPolicy       `yaml:"approve,omitempty"`
	RequireApproval    ProjectApprovalPolicy       `yaml:"require_approval,omitempty"`
	Allow              []ProjectPolicyBinding      `yaml:"allow,omitempty"`
	Deny               []ProjectPolicyBinding      `yaml:"deny,omitempty"`
	DataClassification ProjectClassificationPolicy `yaml:"data_classification,omitempty"`
	Tools              ProjectToolPolicy           `yaml:"tools,omitempty"`
}

type ProjectApprovalPolicy struct {
	Capabilities []string `yaml:"capabilities,omitempty"`
	Tools        []string `yaml:"tools,omitempty"`
}

type ProjectPolicyBinding struct {
	Agent      string `yaml:"agent,omitempty"`
	Team       string `yaml:"team,omitempty"`
	Capability string `yaml:"capability,omitempty"`
	Tool       string `yaml:"tool,omitempty"`
}

type ProjectClassificationPolicy struct {
	Allow []ProjectClassificationBinding `yaml:"allow,omitempty"`
	Deny  []ProjectClassificationBinding `yaml:"deny,omitempty"`
}

type ProjectClassificationBinding struct {
	Agent           string   `yaml:"agent,omitempty"`
	Team            string   `yaml:"team,omitempty"`
	Classification  string   `yaml:"classification,omitempty"`
	Classifications []string `yaml:"classifications,omitempty"`
}

type ProjectToolPolicy struct {
	Allow []ProjectToolBinding `yaml:"allow,omitempty"`
	Deny  []ProjectToolBinding `yaml:"deny,omitempty"`
}

type ProjectToolBinding struct {
	Agent string `yaml:"agent,omitempty"`
	Team  string `yaml:"team,omitempty"`
	Tool  string `yaml:"tool,omitempty"`
}

func projectCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "project", Short: "Project configuration operations"}
	cmd.AddCommand(projectInitCmd(), projectValidateCmd(), projectDiffCmd(), projectApplyCmd(), projectSyncCmd())
	return cmd
}

func projectInitCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := resolveProjectPath(true)
			if err != nil {
				return err
			}
			if _, err := os.Stat(path); err == nil && !force {
				return fmt.Errorf("%s already exists; use --force to overwrite", path)
			}
			cfg := emptyProjectConfig()
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Created %s\n", path)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing project file")
	return cmd
}

func projectValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s is valid\n", path)
			return nil
		},
	}
}

func projectDiffCmd() *cobra.Command {
	var allTenants bool
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Show project changes against Janus API",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			for _, tenant := range tenants {
				if err := diffProjectTenant(cmd, cfg, tenant); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Diff every tenant in the project file")
	return cmd
}

func projectApplyCmd() *cobra.Command {
	var allTenants bool
	var continueOnError bool
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply janus.project.yaml to Janus API",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			var firstErr error
			for _, tenant := range tenants {
				if err := applyProjectTenant(cmd, cfg, tenant); err != nil {
					if !continueOnError {
						return err
					}
					fmt.Fprintf(cmd.ErrOrStderr(), "tenant %s failed: %v\n", tenant, err)
					if firstErr == nil {
						firstErr = err
					}
				}
			}
			return firstErr
		},
	}
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Apply every tenant in the project file")
	cmd.Flags().BoolVar(&continueOnError, "continue-on-error", false, "Continue applying remaining tenants after an error")
	return cmd
}

func projectSyncCmd() *cobra.Command {
	var overwrite bool
	var allTenants bool
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Merge current Janus API resources into janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			for _, tenant := range tenants {
				if err := syncProjectTenant(cmd.Context(), cfg, tenant, overwrite); err != nil {
					return err
				}
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Synced %s\n", path)
			return nil
		},
	}
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing local tenant resource entries")
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Sync every tenant in the project file")
	return cmd
}

func tenantCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tenant", Short: "Tenant project operations"}
	cmd.AddCommand(tenantAddCmd())
	return cmd
}

func tenantAddCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "add [tenant-id]",
		Short: "Create a tenant and persist it to janus.project.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := strings.TrimSpace(args[0])
			if id == "" {
				return fmt.Errorf("tenant id is required")
			}
			if name == "" {
				name = id
			}
			cfg, path, err := loadProjectConfig(true)
			if err != nil {
				return err
			}
			cfg.normalize()
			if _, ok := cfg.Tenants[id]; ok {
				return fmt.Errorf("tenant %s already exists in %s", id, path)
			}
			if err := client().CreateTenant(cmd.Context(), id, name); err != nil {
				return err
			}
			cfg.Tenants[id] = &ProjectTenant{Name: name}
			if cfg.DefaultTenant == "" {
				cfg.DefaultTenant = id
			}
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Tenant %s added and saved to %s\n", id, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Tenant display name")
	return cmd
}

func agentAddCmd() *cobra.Command {
	var name, team, protocol, endpoint, description, mailboxID string
	var capabilities []string
	var classifications []string
	var concurrency, rpm, tpm int
	cmd := &cobra.Command{
		Use:   "add [agent-id]",
		Short: "Register an agent and persist it to janus.project.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agentID := strings.TrimSpace(args[0])
			if agentID == "" {
				return fmt.Errorf("agent id is required")
			}
			if len(capabilities) == 0 {
				return fmt.Errorf("--capability is required")
			}
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, false)
			if err != nil {
				return err
			}
			tenant := tenants[0]
			pt := cfg.Tenants[tenant]
			if pt.Agents == nil {
				pt.Agents = make(map[string]*ProjectAgent)
			}
			if _, ok := pt.Agents[agentID]; ok {
				return fmt.Errorf("agent %s already exists in tenant %s", agentID, tenant)
			}
			caps := make([]ProjectCapability, 0, len(capabilities))
			for _, cap := range capabilities {
				caps = append(caps, ProjectCapability{
					ID:                  strings.TrimSpace(cap),
					DataClassifications: cleanStringSlice(classifications),
				})
			}
			agent := &ProjectAgent{
				Name:         name,
				Team:         team,
				Protocol:     protocol,
				Endpoint:     endpoint,
				Description:  description,
				Capabilities: caps,
				Concurrency:  concurrency,
				RPM:          rpm,
				TPM:          tpm,
			}
			if mailboxID != "" {
				agent.Mailbox = &ProjectMailbox{ID: mailboxID}
			}
			c := projectClient(tenant)
			req, err := registerAgentRequest(agentID, *agent, cfg.Defaults)
			if err != nil {
				return err
			}
			if err := c.RegisterAgent(cmd.Context(), req); err != nil {
				return err
			}
			if _, err := c.CreateMailboxWithConfig(cmd.Context(), mailboxRequest(agentID, *agent, cfg.Defaults)); err != nil {
				return err
			}
			pt.Agents[agentID] = agent
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Agent %s added to tenant %s and saved to %s\n", agentID, tenant, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Agent display name")
	cmd.Flags().StringVar(&team, "team", "", "Team ID")
	cmd.Flags().StringVar(&protocol, "protocol", "", "Protocol (defaults to project defaults or custom-sdk)")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "Agent endpoint")
	cmd.Flags().StringVar(&description, "description", "", "Agent description")
	cmd.Flags().StringArrayVar(&capabilities, "capability", nil, "Capability name; repeat for multiple capabilities")
	cmd.Flags().StringArrayVar(&classifications, "classification", nil, "Allowed data classification for each capability; repeat for multiple values")
	cmd.Flags().IntVar(&concurrency, "concurrency", 0, "Agent and default mailbox max concurrency")
	cmd.Flags().IntVar(&rpm, "rpm", 0, "Agent RPM metadata")
	cmd.Flags().IntVar(&tpm, "tpm", 0, "Agent TPM metadata")
	cmd.Flags().StringVar(&mailboxID, "mailbox", "", "Mailbox ID; defaults to <agent-id>.default")
	return cmd
}

func resolveProjectPath(create bool) (string, error) {
	if projectFile != "" {
		return filepath.Abs(projectFile)
	}
	if env := strings.TrimSpace(os.Getenv("JANUS_PROJECT_FILE")); env != "" {
		return filepath.Abs(env)
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if found, ok := findProjectFile(wd); ok {
		return found, nil
	}
	if create {
		return filepath.Join(wd, defaultProjectFileName), nil
	}
	return "", fmt.Errorf("%s not found; run `janus project init` or `janus tenant add` first", defaultProjectFileName)
}

func findProjectFile(start string) (string, bool) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, defaultProjectFileName)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func loadProjectConfig(create bool) (*ProjectConfig, string, error) {
	path, err := resolveProjectPath(create)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && create {
			return emptyProjectConfig(), path, nil
		}
		return nil, "", err
	}
	cfg := emptyProjectConfig()
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, "", fmt.Errorf("parse %s: %w", path, err)
		}
	}
	cfg.normalize()
	return cfg, path, nil
}

func saveProjectConfig(path string, cfg *ProjectConfig) error {
	cfg.normalize()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal project config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func emptyProjectConfig() *ProjectConfig {
	return &ProjectConfig{
		Version: projectConfigVersion,
		Tenants: map[string]*ProjectTenant{},
	}
}

func (c *ProjectConfig) normalize() {
	if c.Version == "" {
		c.Version = projectConfigVersion
	}
	if c.Tenants == nil {
		c.Tenants = make(map[string]*ProjectTenant)
	}
	for id, tenant := range c.Tenants {
		if tenant == nil {
			c.Tenants[id] = &ProjectTenant{}
		}
	}
}

func validateProjectConfig(cfg *ProjectConfig) error {
	if cfg == nil {
		return fmt.Errorf("project config is required")
	}
	cfg.normalize()
	if cfg.Version != projectConfigVersion {
		return fmt.Errorf("unsupported project config version %q", cfg.Version)
	}
	if cfg.DefaultTenant != "" {
		if _, ok := cfg.Tenants[cfg.DefaultTenant]; !ok {
			return fmt.Errorf("default_tenant %s is not declared", cfg.DefaultTenant)
		}
	}
	for tenantID, tenant := range cfg.Tenants {
		if strings.TrimSpace(tenantID) == "" {
			return fmt.Errorf("tenant id is required")
		}
		if tenant == nil {
			return fmt.Errorf("tenant %s config is required", tenantID)
		}
		for agentID, agent := range tenant.Agents {
			if strings.TrimSpace(agentID) == "" {
				return fmt.Errorf("tenant %s has an empty agent id", tenantID)
			}
			if agent == nil {
				return fmt.Errorf("tenant %s agent %s config is required", tenantID, agentID)
			}
			if agentConcurrency(*agent, cfg.Defaults) < 0 || agent.RPM < 0 || agent.TPM < 0 {
				return fmt.Errorf("tenant %s agent %s has negative limits", tenantID, agentID)
			}
			if len(agent.Capabilities) == 0 {
				return fmt.Errorf("tenant %s agent %s requires at least one capability", tenantID, agentID)
			}
			for _, capability := range agent.Capabilities {
				if strings.TrimSpace(capability.ID) == "" {
					return fmt.Errorf("tenant %s agent %s has an empty capability", tenantID, agentID)
				}
				for _, classification := range capability.DataClassifications {
					if !validProjectClassification(classification) {
						return fmt.Errorf("tenant %s agent %s capability %s has invalid classification %s", tenantID, agentID, capability.ID, classification)
					}
				}
			}
		}
		if err := validateBudgetLimits(tenantID, tenant.Budgets); err != nil {
			return err
		}
		if err := validateProjectPolicies(tenantID, tenant.Policies, cfg.Defaults.Policy.Priority); err != nil {
			return err
		}
	}
	return nil
}

func validateBudgetLimits(tenantID string, budgets ProjectBudgets) error {
	check := func(scope, id string, limits ProjectBudgetLimits) error {
		if limits.RPM < 0 || limits.TPM < 0 || limits.Concurrency < 0 || limits.DailyUSD < 0 || limits.MonthlyUSD < 0 {
			return fmt.Errorf("tenant %s budget %s/%s has negative limits", tenantID, scope, id)
		}
		return nil
	}
	if budgets.Tenant != nil {
		if err := check("tenant", tenantID, *budgets.Tenant); err != nil {
			return err
		}
	}
	for id, limits := range budgets.Teams {
		if err := check("team", id, limits); err != nil {
			return err
		}
	}
	for id, limits := range budgets.Agents {
		if err := check("agent", id, limits); err != nil {
			return err
		}
	}
	for id, limits := range budgets.Models {
		if err := check("model", id, limits); err != nil {
			return err
		}
	}
	for id, limits := range budgets.ModelProviders {
		if err := check("model_provider", id, limits); err != nil {
			return err
		}
	}
	for id, limits := range budgets.Tasks {
		if err := check("task", id, limits); err != nil {
			return err
		}
	}
	return nil
}

func validateProjectPolicies(tenantID string, policies ProjectPolicies, defaultPriority int) error {
	templates, err := policyTemplateRequests(policies, defaultPriority)
	if err != nil {
		return fmt.Errorf("tenant %s policies: %w", tenantID, err)
	}
	seen := make(map[string]struct{})
	for _, template := range templates {
		rule, err := template.BuildPolicyRule(tenantID)
		if err != nil {
			return fmt.Errorf("tenant %s policies: %w", tenantID, err)
		}
		if _, ok := seen[rule.ID]; ok {
			return fmt.Errorf("tenant %s policies contain duplicate generated rule %s", tenantID, rule.ID)
		}
		seen[rule.ID] = struct{}{}
	}
	return nil
}

func validProjectClassification(value string) bool {
	switch strings.TrimSpace(value) {
	case "", "public", "internal", "confidential", "restricted":
		return true
	default:
		return false
	}
}

func selectedProjectTenants(cmd *cobra.Command, cfg *ProjectConfig, all bool) ([]string, error) {
	cfg.normalize()
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("project has no tenants")
	}
	if all {
		names := sortedTenantIDs(cfg)
		return names, nil
	}
	if tenantFlagChanged(cmd) {
		if _, ok := cfg.Tenants[tenantID]; !ok {
			return nil, fmt.Errorf("tenant %s is not declared in project", tenantID)
		}
		return []string{tenantID}, nil
	}
	if cfg.DefaultTenant != "" {
		return []string{cfg.DefaultTenant}, nil
	}
	if len(cfg.Tenants) == 1 {
		for tenant := range cfg.Tenants {
			return []string{tenant}, nil
		}
	}
	if tenantID != "" && tenantID != "default" {
		if _, ok := cfg.Tenants[tenantID]; ok {
			return []string{tenantID}, nil
		}
	}
	return nil, fmt.Errorf("multiple tenants declared; pass --tenant or set default_tenant")
}

func tenantFlagChanged(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	if f := cmd.Flag("tenant"); f != nil && f.Changed {
		return true
	}
	return false
}

func sortedTenantIDs(cfg *ProjectConfig) []string {
	names := make([]string, 0, len(cfg.Tenants))
	for tenant := range cfg.Tenants {
		names = append(names, tenant)
	}
	sort.Strings(names)
	return names
}

func projectClient(projectTenant string) *janus.Client {
	return janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: projectTenant, APIKey: apiKey})
}

func registerAgentRequest(agentID string, agent ProjectAgent, defaults ProjectDefaults) (janus.RegisterAgentRequest, error) {
	displayName := strings.TrimSpace(agent.Name)
	if displayName == "" {
		displayName = agentID
	}
	protocol := strings.TrimSpace(agent.Protocol)
	if protocol == "" {
		protocol = strings.TrimSpace(defaults.Protocol)
	}
	if protocol == "" {
		protocol = "custom-sdk"
	}
	req := janus.RegisterAgentRequest{
		ID:             agentID,
		DisplayName:    displayName,
		TeamID:         strings.TrimSpace(agent.Team),
		Protocol:       protocol,
		Endpoint:       strings.TrimSpace(agent.Endpoint),
		Description:    strings.TrimSpace(agent.Description),
		MaxConcurrency: agentConcurrency(agent, defaults),
		RPM:            agent.RPM,
		TPM:            agent.TPM,
	}
	for _, capability := range agent.Capabilities {
		schema, err := capabilitySchemaJSON(capability)
		if err != nil {
			return janus.RegisterAgentRequest{}, err
		}
		req.Capabilities = append(req.Capabilities, janus.RegisterAgentCapability{
			Capability:  strings.TrimSpace(capability.ID),
			Description: strings.TrimSpace(capability.Description),
			Schema:      schema,
		})
	}
	return req, nil
}

func capabilitySchemaJSON(capability ProjectCapability) (string, error) {
	if len(capability.DataClassifications) == 0 {
		return "", nil
	}
	schema := map[string][]string{
		"allowed_data_classifications": cleanStringSlice(capability.DataClassifications),
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func mailboxRequest(agentID string, agent ProjectAgent, defaults ProjectDefaults) janus.CreateMailboxRequest {
	mailbox := ProjectMailbox{}
	if agent.Mailbox != nil {
		mailbox = *agent.Mailbox
	}
	id := strings.TrimSpace(mailbox.ID)
	if id == "" {
		id = agentID + ".default"
	}
	concurrency := mailbox.Concurrency
	if concurrency <= 0 {
		concurrency = agentConcurrency(agent, defaults)
	}
	ackWait := mailbox.ACKWaitSeconds
	if ackWait <= 0 {
		ackWait = defaults.Mailbox.ACKWaitSeconds
	}
	maxDeliver := mailbox.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = defaults.Mailbox.MaxDeliver
	}
	retention := mailbox.RetentionSeconds
	if retention <= 0 {
		retention = defaults.Mailbox.RetentionSeconds
	}
	return janus.CreateMailboxRequest{
		ID:               id,
		AgentID:          agentID,
		MaxConcurrency:   concurrency,
		ACKWaitSeconds:   ackWait,
		MaxDeliver:       maxDeliver,
		RetentionSeconds: retention,
	}
}

func agentConcurrency(agent ProjectAgent, defaults ProjectDefaults) int {
	if agent.Concurrency > 0 {
		return agent.Concurrency
	}
	if agent.Capacity.MaxConcurrency > 0 {
		return agent.Capacity.MaxConcurrency
	}
	if defaults.Capacity.MaxConcurrency > 0 {
		return defaults.Capacity.MaxConcurrency
	}
	return 1
}

func budgetRequests(tenantID string, budgets ProjectBudgets) []janus.BudgetRequest {
	var out []janus.BudgetRequest
	if budgets.Tenant != nil {
		out = append(out, budgetRequest("tenant", tenantID, *budgets.Tenant))
	}
	appendMapBudgets := func(scope string, values map[string]ProjectBudgetLimits) {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			out = append(out, budgetRequest(scope, key, values[key]))
		}
	}
	appendMapBudgets("team", budgets.Teams)
	appendMapBudgets("agent", budgets.Agents)
	appendMapBudgets("model_provider", budgets.ModelProviders)
	appendMapBudgets("model", budgets.Models)
	appendMapBudgets("task", budgets.Tasks)
	return out
}

func budgetRequest(scope, id string, limits ProjectBudgetLimits) janus.BudgetRequest {
	return janus.BudgetRequest{
		ScopeType:      scope,
		ScopeID:        id,
		RPM:            limits.RPM,
		TPM:            limits.TPM,
		MaxConcurrency: limits.Concurrency,
		DailyCostUSD:   limits.DailyUSD,
		MonthlyCostUSD: limits.MonthlyUSD,
	}
}

func policyTemplateRequests(policies ProjectPolicies, defaultPriority int) ([]core.PolicyRuleTemplateRequest, error) {
	var out []core.PolicyRuleTemplateRequest
	addPriority := func(req core.PolicyRuleTemplateRequest) core.PolicyRuleTemplateRequest {
		if req.Priority == 0 && defaultPriority > 0 {
			req.Priority = defaultPriority
		}
		return req
	}
	appendApproval := func(approval ProjectApprovalPolicy) {
		for _, capability := range cleanStringSlice(approval.Capabilities) {
			out = append(out, addPriority(core.PolicyRuleTemplateRequest{
				Template:   core.PolicyTemplateRequireApprovalCapability,
				Capability: capability,
			}))
		}
		for _, tool := range cleanStringSlice(approval.Tools) {
			out = append(out, addPriority(core.PolicyRuleTemplateRequest{
				Template: core.PolicyTemplateRequireApprovalTool,
				Tool:     tool,
			}))
		}
	}
	appendBindings := func(bindings []ProjectPolicyBinding, allow bool) error {
		for _, binding := range bindings {
			agent := strings.TrimSpace(binding.Agent)
			team := strings.TrimSpace(binding.Team)
			capability := strings.TrimSpace(binding.Capability)
			tool := strings.TrimSpace(binding.Tool)
			if agent == "" && team == "" {
				return fmt.Errorf("policy binding requires agent or team")
			}
			if agent != "" && team != "" {
				return fmt.Errorf("policy binding cannot set both agent and team")
			}
			if capability == "" && tool == "" {
				return fmt.Errorf("policy binding requires capability or tool")
			}
			if capability != "" && tool != "" {
				return fmt.Errorf("policy binding cannot set both capability and tool")
			}
			req := core.PolicyRuleTemplateRequest{AgentID: agent, TeamID: team, Capability: capability, Tool: tool}
			switch {
			case capability != "" && agent != "" && allow:
				req.Template = core.PolicyTemplateAllowAgentCapability
			case capability != "" && agent != "":
				req.Template = core.PolicyTemplateDenyAgentCapability
			case capability != "" && allow:
				req.Template = core.PolicyTemplateAllowTeamCapability
			case capability != "":
				req.Template = core.PolicyTemplateDenyTeamCapability
			case tool != "" && agent != "" && allow:
				req.Template = core.PolicyTemplateAllowAgentTool
			case tool != "" && agent != "":
				req.Template = core.PolicyTemplateDenyAgentTool
			case tool != "" && allow:
				req.Template = core.PolicyTemplateAllowTeamTool
			default:
				req.Template = core.PolicyTemplateDenyTeamTool
			}
			out = append(out, addPriority(req))
		}
		return nil
	}
	appendClassifications := func(bindings []ProjectClassificationBinding, allow bool) error {
		for _, binding := range bindings {
			agent := strings.TrimSpace(binding.Agent)
			team := strings.TrimSpace(binding.Team)
			if agent == "" && team == "" {
				return fmt.Errorf("classification policy requires agent or team")
			}
			if agent != "" && team != "" {
				return fmt.Errorf("classification policy cannot set both agent and team")
			}
			classifications := cleanStringSlice(binding.Classifications)
			if binding.Classification != "" {
				classifications = appendUniqueString(classifications, binding.Classification)
			}
			if len(classifications) == 0 {
				return fmt.Errorf("classification policy requires classification")
			}
			for _, classification := range classifications {
				if !validProjectClassification(classification) {
					return fmt.Errorf("invalid classification %s", classification)
				}
				req := core.PolicyRuleTemplateRequest{
					AgentID:            agent,
					TeamID:             team,
					DataClassification: classification,
				}
				switch {
				case agent != "" && allow:
					req.Template = core.PolicyTemplateAllowAgentDataClassification
				case agent != "":
					req.Template = core.PolicyTemplateDenyAgentDataClassification
				case allow:
					req.Template = core.PolicyTemplateAllowTeamDataClassification
				default:
					req.Template = core.PolicyTemplateDenyTeamDataClassification
				}
				out = append(out, addPriority(req))
			}
		}
		return nil
	}
	appendTools := func(bindings []ProjectToolBinding, allow bool) error {
		converted := make([]ProjectPolicyBinding, 0, len(bindings))
		for _, binding := range bindings {
			converted = append(converted, ProjectPolicyBinding{Agent: binding.Agent, Team: binding.Team, Tool: binding.Tool})
		}
		return appendBindings(converted, allow)
	}
	appendApproval(policies.Approve)
	appendApproval(policies.RequireApproval)
	if err := appendBindings(policies.Allow, true); err != nil {
		return nil, err
	}
	if err := appendBindings(policies.Deny, false); err != nil {
		return nil, err
	}
	if err := appendClassifications(policies.DataClassification.Allow, true); err != nil {
		return nil, err
	}
	if err := appendClassifications(policies.DataClassification.Deny, false); err != nil {
		return nil, err
	}
	if err := appendTools(policies.Tools.Allow, true); err != nil {
		return nil, err
	}
	if err := appendTools(policies.Tools.Deny, false); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, _ := out[i].BuildPolicyRule("sort")
		right, _ := out[j].BuildPolicyRule("sort")
		return left.ID < right.ID
	})
	return out, nil
}

func diffProjectTenant(cmd *cobra.Command, cfg *ProjectConfig, tenantID string) error {
	tenant := cfg.Tenants[tenantID]
	c := projectClient(tenantID)
	ctx := cmd.Context()
	fmt.Fprintf(cmd.OutOrStdout(), "Tenant %s\n", tenantID)
	tenantExists := true
	if _, err := c.GetTenant(ctx, tenantID); err != nil {
		if isAPIStatus(err, http.StatusNotFound) {
			tenantExists = false
			fmt.Fprintf(cmd.OutOrStdout(), "  + tenant %s\n", tenantID)
		} else {
			return err
		}
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "  = tenant %s\n", tenantID)
	}
	for _, agentID := range sortedAgentIDs(tenant) {
		agent := *tenant.Agents[agentID]
		if !tenantExists {
			fmt.Fprintf(cmd.OutOrStdout(), "  + agent %s\n", agentID)
			fmt.Fprintf(cmd.OutOrStdout(), "  + mailbox %s\n", mailboxRequest(agentID, agent, cfg.Defaults).ID)
			continue
		}
		if _, err := c.GetAgent(ctx, agentID); err != nil {
			if isAPIStatus(err, http.StatusNotFound) {
				fmt.Fprintf(cmd.OutOrStdout(), "  + agent %s\n", agentID)
			} else {
				return err
			}
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  = agent %s\n", agentID)
		}
		mb := mailboxRequest(agentID, agent, cfg.Defaults)
		if existing, err := c.GetMailbox(ctx, mb.ID); err != nil {
			if isAPIStatus(err, http.StatusNotFound) {
				fmt.Fprintf(cmd.OutOrStdout(), "  + mailbox %s\n", mb.ID)
			} else {
				return err
			}
		} else if existing.MaxConcurrency != mb.MaxConcurrency ||
			existing.ACKWaitSeconds != mb.ACKWaitSeconds ||
			existing.MaxDeliver != mb.MaxDeliver ||
			existing.RetentionSeconds != mb.RetentionSeconds {
			fmt.Fprintf(cmd.OutOrStdout(), "  ~ mailbox %s\n", mb.ID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  = mailbox %s\n", mb.ID)
		}
	}
	existingBudgets := map[string]struct{}{}
	if tenantExists {
		budgets, err := c.ListBudgets(ctx)
		if err != nil {
			return err
		}
		for _, budget := range budgets {
			existingBudgets[budget.ScopeType+"/"+budget.ScopeID] = struct{}{}
		}
	}
	for _, req := range budgetRequests(tenantID, tenant.Budgets) {
		key := req.ScopeType + "/" + req.ScopeID
		if _, ok := existingBudgets[key]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  = budget %s\n", key)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  + budget %s\n", key)
		}
	}
	existingPolicies := map[string]struct{}{}
	if tenantExists {
		rules, err := c.ListPolicyRules(ctx)
		if err != nil {
			return err
		}
		for _, rule := range rules {
			existingPolicies[rule.ID] = struct{}{}
		}
	}
	templates, err := policyTemplateRequests(tenant.Policies, cfg.Defaults.Policy.Priority)
	if err != nil {
		return err
	}
	for _, template := range templates {
		rule, err := template.BuildPolicyRule(tenantID)
		if err != nil {
			return err
		}
		if _, ok := existingPolicies[rule.ID]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  = policy %s\n", rule.ID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  + policy %s\n", rule.ID)
		}
	}
	return nil
}

func applyProjectTenant(cmd *cobra.Command, cfg *ProjectConfig, tenantID string) error {
	tenant := cfg.Tenants[tenantID]
	c := projectClient(tenantID)
	ctx := cmd.Context()
	fmt.Fprintf(cmd.OutOrStdout(), "Applying tenant %s\n", tenantID)
	if _, err := c.GetTenant(ctx, tenantID); err != nil {
		if !isAPIStatus(err, http.StatusNotFound) {
			return err
		}
		name := tenant.Name
		if name == "" {
			name = tenantID
		}
		if err := c.CreateTenant(ctx, tenantID, name); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  created tenant %s\n", tenantID)
	}
	for _, agentID := range sortedAgentIDs(tenant) {
		agent := *tenant.Agents[agentID]
		if _, err := c.GetAgent(ctx, agentID); err != nil {
			if !isAPIStatus(err, http.StatusNotFound) {
				return err
			}
			req, err := registerAgentRequest(agentID, agent, cfg.Defaults)
			if err != nil {
				return err
			}
			if err := c.RegisterAgent(ctx, req); err != nil && !isAPIStatus(err, http.StatusConflict) {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  created agent %s\n", agentID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  existing agent %s\n", agentID)
		}
		mb := mailboxRequest(agentID, agent, cfg.Defaults)
		if _, err := c.GetMailbox(ctx, mb.ID); err != nil {
			if !isAPIStatus(err, http.StatusNotFound) {
				return err
			}
			if _, err := c.CreateMailboxWithConfig(ctx, mb); err != nil && !isAPIStatus(err, http.StatusConflict) {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  created mailbox %s\n", mb.ID)
		} else {
			update := janus.UpdateMailboxRequest{
				MaxConcurrency:   intPtr(mb.MaxConcurrency),
				ACKWaitSeconds:   intPtr(mb.ACKWaitSeconds),
				MaxDeliver:       intPtr(mb.MaxDeliver),
				RetentionSeconds: intPtr(mb.RetentionSeconds),
			}
			if _, err := c.UpdateMailbox(ctx, mb.ID, update); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  updated mailbox %s\n", mb.ID)
		}
	}
	for _, req := range budgetRequests(tenantID, tenant.Budgets) {
		if _, err := c.UpsertBudget(ctx, req); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  upserted budget %s/%s\n", req.ScopeType, req.ScopeID)
	}
	existingPolicies := map[string]struct{}{}
	rules, err := c.ListPolicyRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		existingPolicies[rule.ID] = struct{}{}
	}
	templates, err := policyTemplateRequests(tenant.Policies, cfg.Defaults.Policy.Priority)
	if err != nil {
		return err
	}
	for _, template := range templates {
		rule, err := template.BuildPolicyRule(tenantID)
		if err != nil {
			return err
		}
		if _, ok := existingPolicies[rule.ID]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  existing policy %s\n", rule.ID)
			continue
		}
		if _, err := c.CreatePolicyRuleFromTemplate(ctx, template); err != nil && !isAPIStatus(err, http.StatusConflict) {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  created policy %s\n", rule.ID)
	}
	return nil
}

func syncProjectTenant(ctx context.Context, cfg *ProjectConfig, tenantID string, overwrite bool) error {
	c := projectClient(tenantID)
	remoteTenant, err := c.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	cfg.normalize()
	pt := cfg.Tenants[tenantID]
	if pt == nil {
		pt = &ProjectTenant{}
		cfg.Tenants[tenantID] = pt
	}
	if pt.Name == "" || overwrite {
		pt.Name = remoteTenant.Name
	}
	if cfg.DefaultTenant == "" {
		cfg.DefaultTenant = tenantID
	}
	agents, err := c.ListAgents(ctx)
	if err != nil {
		return err
	}
	if pt.Agents == nil {
		pt.Agents = make(map[string]*ProjectAgent)
	}
	for _, agent := range agents {
		if _, ok := pt.Agents[agent.ID]; ok && !overwrite {
			continue
		}
		pt.Agents[agent.ID] = projectAgentFromCore(agent)
	}
	budgets, err := c.ListBudgets(ctx)
	if err != nil {
		return err
	}
	mergeBudgetsIntoProject(&pt.Budgets, budgets, overwrite)
	rules, err := c.ListPolicyRules(ctx)
	if err != nil {
		return err
	}
	mergePolicyRulesIntoProject(&pt.Policies, rules, overwrite)
	return nil
}

func projectAgentFromCore(agent core.Agent) *ProjectAgent {
	out := &ProjectAgent{
		Name:        agent.DisplayName,
		Team:        agent.TeamID,
		Protocol:    string(agent.Protocol),
		Endpoint:    agent.Endpoint,
		Description: agent.Description,
		Concurrency: agent.MaxConcurrency,
		RPM:         agent.RPM,
		TPM:         agent.TPM,
	}
	for _, capability := range agent.Capabilities {
		out.Capabilities = append(out.Capabilities, projectCapabilityFromCore(capability))
	}
	return out
}

func projectCapabilityFromCore(capability core.AgentCapability) ProjectCapability {
	out := ProjectCapability{ID: capability.Capability, Description: capability.Description}
	var schema map[string][]string
	if capability.Schema != "" && json.Unmarshal([]byte(capability.Schema), &schema) == nil {
		for _, key := range []string{"allowed_data_classifications", "data_classifications"} {
			out.DataClassifications = append(out.DataClassifications, schema[key]...)
		}
		out.DataClassifications = cleanStringSlice(out.DataClassifications)
	}
	return out
}

func mergeBudgetsIntoProject(target *ProjectBudgets, budgets []janus.BudgetSpec, overwrite bool) {
	for _, budget := range budgets {
		limits := ProjectBudgetLimits{
			RPM:         budget.RPM,
			TPM:         budget.TPM,
			Concurrency: budget.MaxConcurrency,
			DailyUSD:    budget.DailyCostUSD,
			MonthlyUSD:  budget.MonthlyCostUSD,
		}
		switch budget.ScopeType {
		case "tenant":
			if target.Tenant == nil || overwrite {
				target.Tenant = &limits
			}
		case "team":
			if target.Teams == nil {
				target.Teams = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Teams[budget.ScopeID]; !ok || overwrite {
				target.Teams[budget.ScopeID] = limits
			}
		case "agent":
			if target.Agents == nil {
				target.Agents = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Agents[budget.ScopeID]; !ok || overwrite {
				target.Agents[budget.ScopeID] = limits
			}
		case "model_provider":
			if target.ModelProviders == nil {
				target.ModelProviders = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.ModelProviders[budget.ScopeID]; !ok || overwrite {
				target.ModelProviders[budget.ScopeID] = limits
			}
		case "model":
			if target.Models == nil {
				target.Models = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Models[budget.ScopeID]; !ok || overwrite {
				target.Models[budget.ScopeID] = limits
			}
		case "task":
			if target.Tasks == nil {
				target.Tasks = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Tasks[budget.ScopeID]; !ok || overwrite {
				target.Tasks[budget.ScopeID] = limits
			}
		}
	}
}

func mergePolicyRulesIntoProject(target *ProjectPolicies, rules []core.PolicyRule, overwrite bool) {
	if overwrite {
		*target = ProjectPolicies{}
	}
	for _, rule := range rules {
		mergePolicyRuleIntoProject(target, rule)
	}
}

func mergePolicyRuleIntoProject(target *ProjectPolicies, rule core.PolicyRule) {
	condition := map[string]interface{}{}
	action := map[string]interface{}{}
	if json.Unmarshal(rule.Condition, &condition) != nil || json.Unmarshal(rule.Action, &action) != nil {
		return
	}
	decision := stringValue(action["decision"])
	actionName := stringValue(condition["action"])
	resourceType := stringValue(condition["resource.type"])
	resourceValue := stringValue(condition["resource.value"])
	agentID := stringValue(condition["actor.id"])
	teamID := stringValue(condition["actor.team_id"])
	switch decision {
	case string(core.PolicyDecisionApprovalRequired):
		if actionName == "task.publish" && resourceType == "capability" {
			target.Approve.Capabilities = appendUniqueString(target.Approve.Capabilities, resourceValue)
		}
		if actionName == "tool.invoke" && resourceType == "tool" {
			target.Approve.Tools = appendUniqueString(target.Approve.Tools, resourceValue)
		}
	case string(core.PolicyDecisionAllow), string(core.PolicyDecisionDeny):
		allow := decision == string(core.PolicyDecisionAllow)
		if actionName == "task.publish" && resourceType == "capability" {
			binding := ProjectPolicyBinding{Agent: agentID, Team: teamID, Capability: resourceValue}
			if allow {
				target.Allow = appendUniquePolicyBinding(target.Allow, binding)
			} else {
				target.Deny = appendUniquePolicyBinding(target.Deny, binding)
			}
			return
		}
		if actionName == "tool.invoke" && resourceType == "tool" {
			binding := ProjectToolBinding{Agent: agentID, Team: teamID, Tool: resourceValue}
			if allow {
				target.Tools.Allow = appendUniqueToolBinding(target.Tools.Allow, binding)
			} else {
				target.Tools.Deny = appendUniqueToolBinding(target.Tools.Deny, binding)
			}
			return
		}
		if actionName == "task.route" {
			binding := ProjectClassificationBinding{
				Agent:          stringValue(condition["context.target_agent_id"]),
				Team:           stringValue(condition["context.target_team_id"]),
				Classification: stringValue(condition["context.data_classification"]),
			}
			if allow {
				target.DataClassification.Allow = appendUniqueClassificationBinding(target.DataClassification.Allow, binding)
			} else {
				target.DataClassification.Deny = appendUniqueClassificationBinding(target.DataClassification.Deny, binding)
			}
		}
	}
}

func sortedAgentIDs(tenant *ProjectTenant) []string {
	names := make([]string, 0, len(tenant.Agents))
	for agent := range tenant.Agents {
		names = append(names, agent)
	}
	sort.Strings(names)
	return names
}

func isAPIStatus(err error, status int) bool {
	if err == nil {
		return false
	}
	var apiErr *janus.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == status {
		return true
	}
	return false
}

func intPtr(v int) *int {
	if v <= 0 {
		return nil
	}
	return &v
}

func cleanStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniquePolicyBinding(values []ProjectPolicyBinding, value ProjectPolicyBinding) []ProjectPolicyBinding {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueToolBinding(values []ProjectToolBinding, value ProjectToolBinding) []ProjectToolBinding {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueClassificationBinding(values []ProjectClassificationBinding, value ProjectClassificationBinding) []ProjectClassificationBinding {
	for _, existing := range values {
		if existing.Agent == value.Agent &&
			existing.Team == value.Team &&
			existing.Classification == value.Classification &&
			strings.Join(cleanStringSlice(existing.Classifications), "\x00") == strings.Join(cleanStringSlice(value.Classifications), "\x00") {
			return values
		}
	}
	return append(values, value)
}

func stringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
