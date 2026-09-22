package main

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultProjectFileName = "janus.project.yaml"
	projectConfigVersion   = "v1"
)

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

func emptyProjectConfig() *ProjectConfig {
	return &ProjectConfig{
		Version: projectConfigVersion,
		Tenants: map[string]*ProjectTenant{},
	}
}
