package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveProjectPath_ExplicitFile(t *testing.T) {
	projectFile = "/tmp/test-project.yaml"
	defer func() { projectFile = "" }()
	path, err := resolveProjectPath(false)
	require.NoError(t, err)
	assert.Equal(t, "/tmp/test-project.yaml", path)
}

func TestResolveProjectPath_EnvVar(t *testing.T) {
	projectFile = ""
	t.Setenv("JANUS_PROJECT_FILE", "/tmp/env-project.yaml")
	path, err := resolveProjectPath(false)
	require.NoError(t, err)
	assert.Equal(t, "/tmp/env-project.yaml", path)
}

func TestResolveProjectPath_NotFound(t *testing.T) {
	projectFile = ""
	t.Setenv("JANUS_PROJECT_FILE", "")
	tmpDir := t.TempDir()
	wd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(wd)

	_, err := resolveProjectPath(false)
	assert.Error(t, err)
}

func TestResolveProjectPath_CreateNew(t *testing.T) {
	projectFile = ""
	t.Setenv("JANUS_PROJECT_FILE", "")
	tmpDir := t.TempDir()
	wd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(wd)

	path, err := resolveProjectPath(true)
	require.NoError(t, err)
	assert.Contains(t, path, defaultProjectFileName)
}

func TestFindProjectFile_FoundInCurrentDir(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	os.WriteFile(yamlPath, []byte("version: \"1\""), 0644)

	found, ok := findProjectFile(tmpDir)
	assert.True(t, ok)
	assert.Equal(t, yamlPath, found)
}

func TestFindProjectFile_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	os.Mkdir(filepath.Join(tmpDir, ".git"), 0755)

	_, ok := findProjectFile(tmpDir)
	assert.False(t, ok)
}

func TestLoadProjectConfig_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	projectFile = filepath.Join(tmpDir, defaultProjectFileName)
	defer func() { projectFile = "" }()

	cfg, path, err := loadProjectConfig(true)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, projectFile, path)
}

func TestLoadProjectConfig_Existing(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	os.WriteFile(yamlPath, []byte(`version: "1"
tenants:
  acme:
    name: Acme
`), 0644)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	cfg, _, err := loadProjectConfig(false)
	require.NoError(t, err)
	assert.Equal(t, "1", cfg.Version)
	assert.Len(t, cfg.Tenants, 1)
}

func TestSaveProjectConfig(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Version = "1"

	err := saveProjectConfig(yamlPath, cfg)
	require.NoError(t, err)

	_, err = os.Stat(yamlPath)
	assert.NoError(t, err)
}

func TestEmptyProjectConfig_Helper(t *testing.T) {
	cfg := emptyProjectConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, "v1", cfg.Version)
}

func TestProjectConfig_NormalizeSortsTenants(t *testing.T) {
	cfg := &ProjectConfig{
		Version: "1",
		Tenants: map[string]*ProjectTenant{
			"z": {Name: "Z Corp"},
			"a": {Name: "A Corp"},
		},
	}
	cfg.normalize()
	assert.Contains(t, cfg.Tenants, "a")
	assert.Contains(t, cfg.Tenants, "z")
}
